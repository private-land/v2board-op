package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/redis/go-redis/v9"
	"github.com/theckman/go-flock"
	"github.com/xflash-panda/v2board-op/internal/app/rotation"
	"io"
	"os"
	"os/signal"
	"syscall"

	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
	"github.com/xflash-panda/v2board-op/internal/app/change_ip"
	"github.com/xflash-panda/v2board-op/internal/pkg/api"
	"github.com/xflash-panda/v2board-op/internal/pkg/service"
	"github.com/xflash-panda/v2board-op/internal/pkg/util"
)

const (
	Name                       = "change-ip(lightsail-cf)"
	Version                    = "0.6.0"
	CopyRight                  = "XFLASH-PANDA@2023"
	DefaultJobMaxTryNum    int = 5
	DefaultLightsailRegion     = "ap-northeast-1"
)

func init() {
	cli.VersionFlag = &cli.BoolFlag{
		Name:    "version",
		Aliases: []string{"V"},
		Usage:   "print only the version",
	}

	cli.ErrWriter = io.Discard
	cli.VersionPrinter = func(c *cli.Context) {
		fmt.Printf("version=%s ", Version)
	}
}

func main() {
	var apiConfig api.Config
	var lightsailCFJobConfig change_ip.LightsailCFJobConfig
	var lightsailSrvConfig service.LightsailSrvConfig
	var cloudflreSrvConfig service.CloudflareSrvConfig

	var logLevel string
	var jobMaxTryNum int
	defaultServerQueryTags := cli.NewStringSlice("lightsail_cf_ap-northeast-1")
	serverQueryTags := cli.NewStringSlice()

	app := &cli.App{
		Name:        Name,
		Version:     Version,
		Copyright:   CopyRight,
		Usage:       "Automatically change IP script for the v2Board",
		Description: "This script is only used for nodes using lightsail and cloudflare dns",
		Flags: []cli.Flag{
			&cli.BoolFlag{Name: "daemon", Usage: "Enable durable rotation daemon (off by default)", EnvVars: []string{"ROTATION_DAEMON"}},
			&cli.StringFlag{Name: "rotation_redis_addr", Value: "127.0.0.1:6379", EnvVars: []string{"ROTATION_REDIS_ADDR"}},
			&cli.StringFlag{Name: "rotation_redis_username", EnvVars: []string{"ROTATION_REDIS_USERNAME"}},
			&cli.StringFlag{Name: "rotation_redis_password", EnvVars: []string{"ROTATION_REDIS_PASSWORD"}},
			&cli.IntFlag{Name: "rotation_redis_db", EnvVars: []string{"ROTATION_REDIS_DB"}},
			&cli.BoolFlag{Name: "rotation_redis_tls", EnvVars: []string{"ROTATION_REDIS_TLS"}},
			&cli.StringFlag{Name: "worker_id", EnvVars: []string{"ROTATION_WORKER_ID"}},
			&cli.StringFlag{
				Name:        "api",
				Usage:       "Server address",
				EnvVars:     []string{"X_API", "API"},
				Required:    true,
				Destination: &apiConfig.APIHost,
			},
			&cli.StringFlag{
				Name:        "token",
				Usage:       "Token of server API",
				EnvVars:     []string{"X_TOKEN", "TOKEN"},
				Required:    true,
				Destination: &apiConfig.Token,
			},
			&cli.StringSliceFlag{
				Name:        "server_query_tags",
				Value:       defaultServerQueryTags,
				Usage:       "Used for internal query filtering nodes",
				EnvVars:     []string{"X_SERVER_QUERY_TAGS", "QUERY_TAGS"},
				Destination: serverQueryTags,
				Required:    true,
			},
			&cli.StringFlag{
				Name:        "cloudflare_email",
				Usage:       "Cloudflare Email",
				EnvVars:     []string{"X_CLOUDFLARE_EMAIL", "CLOUDFLARE_EMAIL"},
				Destination: &cloudflreSrvConfig.Email,
				Required:    true,
			},
			&cli.StringFlag{
				Name:        "cloudflare_key",
				Usage:       "Cloudflare Key",
				EnvVars:     []string{"X_CLOUDFLARE_KEY", "CLOUDFLARE_KEY"},
				Destination: &cloudflreSrvConfig.Key,
				Required:    true,
			},
			&cli.StringFlag{
				Name:        "lightsail_region",
				Value:       DefaultLightsailRegion,
				Usage:       "Configure the region that lightsail queries",
				EnvVars:     []string{"X_LIGHTSAIL_REGION", "LIGHTSAIL_REGION"},
				Destination: &lightsailSrvConfig.Region,
				Required:    false,
			},
			&cli.StringFlag{
				Name:        "aws_key",
				Usage:       "AWS Key",
				EnvVars:     []string{"X_AWS_KEY", "AWS_KEY"},
				Destination: &lightsailSrvConfig.Key,
				Required:    false,
			},
			&cli.StringFlag{
				Name:        "aws_secret",
				Usage:       "AWS Secret Key",
				EnvVars:     []string{"X_AWS_SECRET", "AWS_SECRET"},
				Destination: &lightsailSrvConfig.Secret,
				Required:    false,
			},
			&cli.IntFlag{
				Name:        "max_num",
				Value:       DefaultJobMaxTryNum,
				Usage:       "Maximum number of job attempts",
				EnvVars:     []string{"X_MAX_NUM", "MAX_NUM"},
				Destination: &jobMaxTryNum,
				Required:    false,
			},
			&cli.IntFlag{
				Name:        "concurrency",
				Value:       3,
				Usage:       "Maximum number of concurrent IP changes per batch",
				EnvVars:     []string{"X_CONCURRENCY", "CONCURRENCY"},
				Destination: &lightsailCFJobConfig.Concurrency,
				Required:    false,
			},
			&cli.StringFlag{
				Name:        "log_mode",
				Value:       "error",
				Usage:       "Log mode",
				EnvVars:     []string{"X_LOG_LEVEL", "LOG_LEVEL"},
				Destination: &logLevel,
				Required:    false,
			},
		},
		Before: func(c *cli.Context) error {
			return util.SetupLogger(logLevel)
		},
		Action: func(c *cli.Context) error {
			if c.Bool("daemon") {
				ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
				defer cancel()
				// The same regional/tag lock used by legacy cron fences mixed deployments.
				lock := flock.New(change_ip.RotationLockPath(serverQueryTags.Value()))
				ok, err := lock.TryLock()
				if err != nil {
					return err
				}
				if !ok {
					return fmt.Errorf("regional rotation lock is held")
				}
				defer func() { _ = lock.Unlock() }()
				options := &redis.Options{Addr: c.String("rotation_redis_addr"), Username: c.String("rotation_redis_username"), Password: c.String("rotation_redis_password"), DB: c.Int("rotation_redis_db")}
				if c.Bool("rotation_redis_tls") {
					options.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
				}
				redisClient := redis.NewClient(options)
				defer func() { _ = redisClient.Close() }()
				worker := c.String("worker_id")
				if worker == "" {
					host, _ := os.Hostname()
					worker = fmt.Sprintf("%s-%d", host, os.Getpid())
				}
				daemon := rotation.Daemon{API: &rotation.Client{BaseURL: apiConfig.APIHost, Token: apiConfig.Token}, Wake: &rotation.RedisWake{Client: redisClient}, Tags: serverQueryTags.Value(), WorkerID: worker, Concurrency: lightsailCFJobConfig.Concurrency, NewCloud: func() (rotation.Cloud, error) { return rotation.NewSDKCloud(lightsailSrvConfig, cloudflreSrvConfig) }}
				return daemon.Run(ctx)
			}
			log.Debugln("api host: ", apiConfig.APIHost)
			log.Debugln("api token: ", apiConfig.Token)
			if len(lightsailSrvConfig.Key) > 0 {
				log.Debugln("aws key: ", lightsailSrvConfig.Key)
			}
			if len(lightsailSrvConfig.Secret) > 0 {
				log.Debugln("aws secret: ", lightsailSrvConfig.Secret)
			}
			log.Debugln("cloudflare email: ", cloudflreSrvConfig.Email)
			log.Debugln("max num: ", jobMaxTryNum)
			log.Debugln("server query tags: ", serverQueryTags)
			log.Debugln("lightsail region: ", lightsailSrvConfig.Region)

			if logLevel != "debug" {
				defer func() {
					if e := recover(); e != nil {
						panic(e)
					}
				}()
			}

			s := make(chan os.Signal, 1)
			signal.Notify(s, os.Interrupt, syscall.SIGTERM)
			go func() {
				for {
					<-s
					log.Warningln("The program cannot be interrupted")
				}
			}()

			lightSailSrv, err := service.NewLightSailService(&lightsailSrvConfig)
			if err != nil {
				log.Fatal(err)
			}
			cloudflareSrv, err := service.NewCloudflareService(&cloudflreSrvConfig)
			if err != nil {
				log.Fatal(err)
			}

			lightsailCFJobConfig.QueryTags = serverQueryTags.Value()
			apiClient := api.New(&apiConfig)

			tryNum := 0
			for tryNum < jobMaxTryNum {
				tryNum++
				lightsailCFJob := change_ip.NewLightsailCFJob(&lightsailCFJobConfig, apiClient, lightSailSrv, cloudflareSrv)
				if err := lightsailCFJob.Init(); err != nil {
					log.Fatalf("job initialization failure :%s", err)
				}

				log.Infof("job execution times: %d", tryNum)
				rerunStatus, runErr := lightsailCFJob.Run()
				log.Infoln("job execution completed")
				if runErr != nil {
					log.Fatal(runErr)
				}
				if !rerunStatus {
					break
				}
			}

			return nil
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
