package rotation

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/lightsail"
	"github.com/aws/aws-sdk-go-v2/service/lightsail/types"
	"github.com/cloudflare/cloudflare-go"
	"github.com/theckman/go-flock"
	"github.com/xflash-panda/v2board-op/internal/app/change_ip"
	"github.com/xflash-panda/v2board-op/internal/pkg/service"
	"github.com/xflash-panda/v2board-op/internal/pkg/util"
	"net"
	"sync"
	"time"
)

// SDKCloud reuses the established SDK/services. Each task has its own adapter;
// every SDK mutation and wait is bound to its lease's cancellation context.
type SDKCloud struct {
	AWS          *lightsail.Client
	CF           *cloudflare.API
	lock         *flock.Flock
	fence        func(context.Context) error
	StateTimeout time.Duration
}

func NewSDKCloud(ls service.LightsailSrvConfig, cf service.CloudflareSrvConfig) (Cloud, error) {
	s, err := service.NewLightSailService(&ls)
	if err != nil {
		return nil, err
	}
	a, err := s.GetClient()
	if err != nil {
		return nil, err
	}
	d, err := service.NewCloudflareService(&cf)
	if err != nil {
		return nil, err
	}
	c, err := d.GetAPI()
	if err != nil {
		return nil, err
	}
	return &SDKCloud{AWS: a, CF: c}, nil
}
func (c *SDKCloud) instance(ctx context.Context, name string) (*types.Instance, error) {
	o, err := c.AWS.GetInstance(ctx, &lightsail.GetInstanceInput{InstanceName: &name})
	if err != nil {
		return nil, err
	}
	if o.Instance == nil {
		return nil, errors.New("missing instance")
	}
	return o.Instance, nil
}
func (c *SDKCloud) Inspect(ctx context.Context, t *Task) (string, error) {
	if t.InstanceName != "" {
		if err := c.acquire(t.InstanceName); err != nil {
			return "", err
		}
		v, err := c.instance(ctx, t.InstanceName)
		if err != nil {
			return "", err
		}
		if v.State == nil || v.State.Name == nil || *v.State.Name != "running" {
			return "", errors.New("instance state ambiguous; manual inspection required")
		}
		if v.PublicIpAddress == nil {
			return "", errors.New("instance has no IP")
		}
		if *v.PublicIpAddress != t.SourceIP {
			for _, ref := range t.Targets {
				if ref.Type == "mieru" && (v.IsStaticIp == nil || *v.IsStaticIp) {
					return "", errors.New("mieru recovered IP must be dynamic; manual inspection required")
				}
			}
		}
		return *v.PublicIpAddress, nil
	}
	if t.Stage == "rotating" {
		return "", errors.New("rotating task lacks instance identity")
	}
	var token *string
	for {
		out, err := c.AWS.GetInstances(ctx, &lightsail.GetInstancesInput{PageToken: token})
		if err != nil {
			return "", err
		}
		for _, v := range out.Instances {
			if v.PublicIpAddress != nil && *v.PublicIpAddress == t.SourceIP && v.Name != nil {
				t.InstanceName = *v.Name
				if err := c.acquire(t.InstanceName); err != nil {
					return "", err
				}
				fresh, err := c.instance(ctx, t.InstanceName)
				if err != nil {
					return "", err
				}
				if fresh.PublicIpAddress == nil {
					return "", errors.New("instance has no IP")
				}
				return *fresh.PublicIpAddress, nil
			}
		}
		token = out.NextPageToken
		if token == nil {
			break
		}
	}
	return "", ErrStaleSource
}
func (c *SDKCloud) Rotate(ctx context.Context, t *Task) (string, error) {
	v, err := c.instance(ctx, t.InstanceName)
	if err != nil {
		return "", err
	}
	if v.PublicIpAddress == nil || *v.PublicIpAddress != t.SourceIP {
		return "", errors.New("source changed before mutation")
	}
	staticMutationMu.Lock()
	staticLocked := true
	defer func() {
		if staticLocked {
			staticMutationMu.Unlock()
		}
	}()
	ips := []types.StaticIp{}
	var token *string
	for {
		out, err := c.AWS.GetStaticIps(ctx, &lightsail.GetStaticIpsInput{PageToken: token})
		if err != nil {
			return "", err
		}
		ips = append(ips, out.StaticIps...)
		token = out.NextPageToken
		if token == nil {
			break
		}
	}
	kind := ""
	for _, ref := range t.Targets {
		if ref.Type == "mieru" {
			kind = "mieru"
			break
		}
	}
	mode := rotationMode(kind, v.IsStaticIp != nil && *v.IsStaticIp, len(ips))
	if mode == "release" || mode == "release_restart" {
		found := false
		for _, ip := range ips {
			if ip.IpAddress != nil && *ip.IpAddress == t.SourceIP {
				if err := c.beforeMutation(ctx); err != nil {
					return "", err
				}
				if _, err = c.AWS.DetachStaticIp(ctx, &lightsail.DetachStaticIpInput{StaticIpName: ip.Name}); err != nil {
					return "", err
				}
				if err := c.beforeMutation(ctx); err != nil {
					return "", err
				}
				if _, err = c.AWS.ReleaseStaticIp(ctx, &lightsail.ReleaseStaticIpInput{StaticIpName: ip.Name}); err != nil {
					return "", err
				}
				found = true
				break
			}
		}
		if !found {
			return "", errors.New("static IP not found")
		}
		if mode == "release" {
			fresh, err := c.instance(ctx, t.InstanceName)
			if err != nil {
				return "", err
			}
			if fresh.PublicIpAddress == nil {
				return "", errors.New("released IP not available")
			}
			return *fresh.PublicIpAddress, nil
		}
	}
	if mode == "allocate" {
		name := t.InstanceName + "-static-ip"
		if err := c.beforeMutation(ctx); err != nil {
			return "", err
		}
		if _, err = c.AWS.AllocateStaticIp(ctx, &lightsail.AllocateStaticIpInput{StaticIpName: &name}); err != nil {
			return "", err
		}
		if err := c.beforeMutation(ctx); err != nil {
			return "", err
		}
		if _, err = c.AWS.AttachStaticIp(ctx, &lightsail.AttachStaticIpInput{InstanceName: &t.InstanceName, StaticIpName: &name}); err != nil {
			return "", err
		}
		out, err := c.AWS.GetStaticIp(ctx, &lightsail.GetStaticIpInput{StaticIpName: &name})
		if err != nil {
			return "", err
		}
		if out.StaticIp == nil || out.StaticIp.IpAddress == nil {
			return "", errors.New("allocated IP missing")
		}
		return *out.StaticIp.IpAddress, nil
	}
	staticMutationMu.Unlock()
	staticLocked = false

	if err := c.beforeMutation(ctx); err != nil {
		return "", err
	}
	if _, err = c.AWS.StopInstance(ctx, &lightsail.StopInstanceInput{InstanceName: &t.InstanceName}); err != nil {
		return "", err
	}
	if err = c.wait(ctx, t.InstanceName, "stopped"); err != nil {
		return "", err
	}
	if err := c.beforeMutation(ctx); err != nil {
		return "", err
	}
	if _, err = c.AWS.StartInstance(ctx, &lightsail.StartInstanceInput{InstanceName: &t.InstanceName}); err != nil {
		return "", err
	}
	if err = c.wait(ctx, t.InstanceName, "running"); err != nil {
		return "", err
	}
	v, err = c.instance(ctx, t.InstanceName)
	if err != nil {
		return "", err
	}
	if v.PublicIpAddress == nil {
		return "", errors.New("new IP missing")
	}
	if v.IsStaticIp != nil && *v.IsStaticIp {
		return "", errors.New("dynamic rotation returned static IP")
	}
	return *v.PublicIpAddress, nil
}
func (c *SDKCloud) wait(ctx context.Context, name, state string) error {
	timeout := c.StateTimeout
	if timeout <= 0 {
		timeout = 5 * time.Minute
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	for {
		out, err := c.AWS.GetInstanceState(ctx, &lightsail.GetInstanceStateInput{InstanceName: &name})
		if err != nil {
			return err
		}
		if out.State != nil && out.State.Name != nil && *out.State.Name == state {
			return nil
		}
		if !pause(ctx, 5*time.Second) {
			return ctx.Err()
		}
	}
}
func (c *SDKCloud) DNS(ctx context.Context, t *Task) error {
	if t.InstanceName == "" {
		return errors.New("saved IP task lacks instance identity")
	}
	if err := c.acquire(t.InstanceName); err != nil {
		return err
	}
	current, err := c.Inspect(ctx, t)
	if err != nil {
		return err
	}
	if current != t.TargetIP {
		return errors.New("saved target IP no longer belongs to instance")
	}
	if net.ParseIP(t.TargetIP) == nil {
		return errors.New("invalid target IP")
	}
	seen := map[string]bool{}
	type deletion struct {
		zone *cloudflare.ResourceContainer
		id   string
	}
	deletes := []deletion{}
	for _, target := range t.Targets {
		host := target.Host
		if seen[host] {
			continue
		}
		seen[host] = true
		root, err := util.GetRootDomain(host)
		if err != nil {
			return err
		}
		zones, err := c.CF.ListZones(ctx, root)
		if err != nil {
			return err
		}
		if len(zones) != 1 {
			return errors.New("DNS zone not unique")
		}
		zone := cloudflare.ZoneIdentifier(zones[0].ID)
		records, _, err := c.CF.ListDNSRecords(ctx, zone, cloudflare.ListDNSRecordsParams{Type: "A", Name: host})
		if err != nil {
			return err
		}
		haveNew := false
		old := []cloudflare.DNSRecord{}
		for _, r := range records {
			if r.Content == t.TargetIP {
				haveNew = true
			}
			if r.Content == t.SourceIP {
				old = append(old, r)
			}
		}
		if !haveNew {
			if len(old) > 0 {
				r := old[0]
				if err := c.beforeMutation(ctx); err != nil {
					return err
				}
				_, err = c.CF.UpdateDNSRecord(ctx, zone, cloudflare.UpdateDNSRecordParams{ID: r.ID, Type: "A", Name: host, Content: t.TargetIP, TTL: r.TTL, Proxied: r.Proxied})
				old = old[1:]
			} else {
				if err := c.beforeMutation(ctx); err != nil {
					return err
				}
				_, err = c.CF.CreateDNSRecord(ctx, zone, cloudflare.CreateDNSRecordParams{Type: "A", Name: host, Content: t.TargetIP, TTL: 60})
			}
			if err != nil {
				return err
			}
		}
		// Re-read the provider before removing any remaining old address records.
		fresh, _, err := c.CF.ListDNSRecords(ctx, zone, cloudflare.ListDNSRecordsParams{Type: "A", Name: host})
		if err != nil {
			return err
		}
		haveNew = false
		for _, r := range fresh {
			if r.Content == t.TargetIP {
				haveNew = true
			}
		}
		if !haveNew {
			return fmt.Errorf("DNS update not verified for %s", host)
		}
		for _, r := range old {
			deletes = append(deletes, deletion{zone, r.ID})
		}
	}
	for _, d := range deletes {
		if err := c.beforeMutation(ctx); err != nil {
			return err
		}
		if err := c.CF.DeleteDNSRecord(ctx, d.zone, d.id); err != nil {
			return err
		}
	}
	return nil
}

func (c *SDKCloud) acquire(name string) error {
	if c.lock != nil {
		return nil
	}
	sum := sha256.Sum256([]byte(name))
	lock := flock.New(fmt.Sprintf("/tmp/rotation_instance_%x.lock", sum[:16]))
	ok, err := lock.TryLock()
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("instance rotation already active")
	}
	c.lock = lock
	return nil
}
func (c *SDKCloud) Close() error {
	if c.lock != nil {
		return c.lock.Unlock()
	}
	return nil
}

var staticMutationMu sync.Mutex

func rotationMode(kind string, static bool, count int) string {
	return change_ip.SelectIPRotation(kind, static, count)
}

func (c *SDKCloud) SetFence(f func(context.Context) error) { c.fence = f }
func (c *SDKCloud) beforeMutation(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if c.fence != nil {
		return c.fence(ctx)
	}
	return nil
}
