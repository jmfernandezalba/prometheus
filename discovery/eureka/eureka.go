// Copyright 2016 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package eureka

import (
	"context"
	"fmt"
	"net"
	"time"
	"strings"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/kangwoo/go-eureka-client/eureka"

	"github.com/prometheus/client_golang/prometheus"
	config_util "github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/discovery/targetgroup"
	"github.com/prometheus/prometheus/util/strutil"
)

const (
	// metaLabelPrefix is the meta prefix used for all meta labels in this discovery.
	metaLabelPrefix = model.MetaLabelPrefix + "eureka_"

	// appLabel is used for the name of the app in Marathon.
	appLabel      model.LabelName = metaLabelPrefix + "app"
	instanceLabel model.LabelName = metaLabelPrefix + "instance"

	// Constants for instrumentation.
	namespace = "prometheus"
)

var (
	refreshFailuresCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "sd_eureka_refresh_failures_total",
			Help:      "The number of Eureka-SD refresh failures.",
		})
	refreshDuration = prometheus.NewSummary(
		prometheus.SummaryOpts{
			Namespace: namespace,
			Name:      "sd_eureka_refresh_duration_seconds",
			Help:      "The duration of a Eureka-SD refresh in seconds.",
		})
	// DefaultSDConfig is the default Eureka SD configuration.
	DefaultSDConfig = SDConfig{
		Timeout:         model.Duration(30 * time.Second),
		RefreshInterval: model.Duration(30 * time.Second),
		MetricsPath:     "/prometheus",
		EnabledOnly:     false,
	}
)

// SDConfig is the configuration for services running on Eureka.
type SDConfig struct {
	Servers         []string              `yaml:"servers"`
	Timeout         model.Duration        `yaml:"timeout,omitempty"`
	RefreshInterval model.Duration        `yaml:"refresh_interval,omitempty"`
	TLSConfig       config_util.TLSConfig `yaml:"tls_config,omitempty"`
	MetricsPath     string                `yaml:"metrics_path,omitempty"`
	EnabledOnly     bool                  `yaml:"enabledOnly,omitempty"`

	// Catches all undefined fields and must be empty after parsing.
	XXX map[string]interface{}            `yaml:",inline"`
}

func checkOverflow(m map[string]interface{}, ctx string) error {
	if len(m) > 0 {
		var keys []string
		for k := range m {
			keys = append(keys, k)
		}
		return fmt.Errorf("unknown fields in %s: %s", ctx, strings.Join(keys, ", "))
	}
	return nil
}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (c *SDConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	*c = DefaultSDConfig
	type plain SDConfig
	err := unmarshal((*plain)(c))
	if err != nil {
		return err
	}
	if err := checkOverflow(c.XXX, "eureka_sd_config"); err != nil {
		return err
	}
	if len(c.Servers) == 0 {
		return fmt.Errorf("Eureka SD config must contain at least one Eureka server")
	}

	return nil
}

func init() {
	prometheus.MustRegister(refreshFailuresCount)
	prometheus.MustRegister(refreshDuration)
}

// Discovery provides service discovery based on a Marathon instance.
type Discovery struct {
	client          *eureka.Client
	servers         []string
	refreshInterval time.Duration
	lastRefresh     map[string]*targetgroup.Group
	metricsPath     string
	enabledOnly     bool
	logger          log.Logger
}

// NewDiscovery returns a new Eureka Service Discovery.
func NewDiscovery(conf SDConfig, logger log.Logger) (*Discovery, error) {
	client := eureka.NewClient(conf.Servers, logger)

	return &Discovery{
		client:          client,
		servers:         conf.Servers,
		refreshInterval: time.Duration(conf.RefreshInterval),
		metricsPath:     conf.MetricsPath,
		enabledOnly:     conf.EnabledOnly,
		logger:          logger,
	}, nil
}

// Run implements the TargetProvider interface.
func (d *Discovery) Run(ctx context.Context, ch chan<- []*targetgroup.Group) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(d.refreshInterval):
			err := d.updateServices(ctx, ch)
			if err != nil {
				level.Error(d.logger).Log("msg", "Error while updating services", "err", err)
			}
		}
	}
}

func (d *Discovery) updateServices(ctx context.Context, ch chan<- []*targetgroup.Group) (err error) {
	t0 := time.Now()
	defer func() {
		refreshDuration.Observe(time.Since(t0).Seconds())
		if err != nil {
			refreshFailuresCount.Inc()
		}
	}()

	targetMap, err := d.fetchTargetGroups()
	if err != nil {
		return err
	}

	all := make([]*targetgroup.Group, 0, len(targetMap))
	for _, tg := range targetMap {
		all = append(all, tg)
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case ch <- all:
	}

	// Remove services which did disappear.
	for source := range d.lastRefresh {
		_, ok := targetMap[source]
		if !ok {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case ch <- []*targetgroup.Group{{Source: source}}:
				level.Debug(d.logger).Log("msg", "Removing group", "source", source)
			}
		}
	}

	d.lastRefresh = targetMap
	return nil
}

func (d *Discovery) fetchTargetGroups() (map[string]*targetgroup.Group, error) {
	apps, err := d.client.GetApplications()
	if err != nil {
		return nil, err
	}

	groups := d.appsToTargetGroups(apps)
	return groups, nil
}

// AppsToTargetGroups takes an array of Eureka Application and converts them into target groups.
func (d *Discovery) appsToTargetGroups(apps *eureka.Applications) map[string]*targetgroup.Group {
	tgroups := map[string]*targetgroup.Group{}
	for _, app := range apps.Applications {
		group := d.createTargetGroup(&app)
		tgroups[group.Source] = group
	}
	return tgroups
}

func (d *Discovery) createTargetGroup(app *eureka.Application) *targetgroup.Group {
	var (
		targets = d.targetsForApp(app)
		appName = model.LabelValue(app.Name)
	)
	tg := &targetgroup.Group{
		Targets: targets,
		Labels: model.LabelSet{
			model.JobLabel: appName,
			model.LabelName("service"): model.LabelValue(strings.ToLower(app.Name)),
		},
		Source: app.Name,
	}

	return tg
}

func (d *Discovery) targetsForApp(app *eureka.Application) []model.LabelSet {
	targets := make([]model.LabelSet, 0, len(app.Instances))
	for _, t := range app.Instances {
		if d.enabledOnly && t.Metadata.Map["prometheusEnabled"] != "true" {
			continue
		}

		targetAddress := targetForInstance(&t)
		target := model.LabelSet{
			model.AddressLabel:     model.LabelValue(targetAddress),
			model.MetricsPathLabel: model.LabelValue(d.metricsPath),
			model.InstanceLabel:    model.LabelValue(t.InstanceId),
		}
		for ln, lv := range t.Metadata.Map {
			ln = strutil.SanitizeLabelName(ln)
			target[model.LabelName(ln)] = model.LabelValue(lv)
		}
		targets = append(targets, target)
	}
	return targets
}

func targetForInstance(instance *eureka.InstanceInfo) string {
	return net.JoinHostPort(instance.HostName, fmt.Sprintf("%d", instance.Port.Port))
}
