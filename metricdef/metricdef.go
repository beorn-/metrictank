/*
 * Copyright (c) 2015, Raintank Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package metricdef

import (
	"bytes"
	"crypto/md5"
	"encoding/json"
	"fmt"
	elastigo "github.com/mattbaird/elastigo/lib"
	"github.com/raintank/raintank-metric/setting"
	"gopkg.in/redis.v2"
	"log"
	"sort"
	"strconv"
	"time"
)

type MetricDefinition struct {
	Id         string            `json:"id"`
	Name       string            `json:"name" elastic:"type:string,index:not_analyzed"`
	OrgId      int               `json:"org_id"`
	Metric     string            `json:"metric"`
	TargetType string            `json:"target_type"` // an emum ["derive","gauge"] in nodejs
	Unit       string            `json:"unit"`
	Interval   int               `json:"interval"`   // minimum 10
	LastUpdate int64             `json:"lastUpdate"` // unix epoch time, per the nodejs definition
	Tags       map[string]string `json:"tags"`
}

type IndvMetric struct {
	Id         string            `json:"id"`
	OrgId      int               `json:"org_id"`
	Name       string            `json:"name"`
	Metric     string            `json:"metric"`
	Interval   int               `json:"interval"`
	Value      float64           `json:"value"`
	Unit       string            `json:"unit"`
	Time       int64             `json:"time"`
	TargetType string            `json:"target_type"`
	Tags       map[string]string `json:"tags"`
}

// sets the ID of the metric.
// the id is in the format  OrgId.md5Sum
// the md5sum is a hash of the the concatination of the
// series name + each tag key:value pair, sorted alphabetically.
func (m *IndvMetric) SetId() {
	if m.Id != "" {
		//id already set.
		return
	}
	var buffer bytes.Buffer
	buffer.WriteString(m.Name)
	keys := make([]string, 0)
	for k, _ := range m.Tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		buffer.WriteString(fmt.Sprintf(":%s=%s", k, m.Tags[k]))
	}

	m.Id = fmt.Sprintf("%d.%x", m.OrgId, md5.Sum(buffer.Bytes()))
}

func (m *IndvMetric) EnsureIndex() error {
	m.SetId()
	def, err := GetMetricDefinition(m.Id)
	if err != nil && err.Error() != "record not found" {
		return err
	}
	//if the definition does not exist, or is older then 10minutes. update it.
	if def == nil || def.LastUpdate < (time.Now().Unix()-600) {
		mdef := &MetricDefinition{
			Id:         m.Id,
			Name:       m.Name,
			OrgId:      m.OrgId,
			Metric:     m.Metric,
			TargetType: m.TargetType,
			Interval:   m.Interval,
			LastUpdate: time.Now().Unix(),
			Unit:       m.Unit,
			Tags:       m.Tags,
		}
		if err := mdef.Save(); err != nil {
			return err
		}
	}
	return nil
}

var es *elastigo.Conn

func InitElasticsearch() error {
	es = elastigo.NewConn()
	es.Domain = setting.Config.ElasticsearchDomain // needs to be configurable obviously
	es.Port = strconv.Itoa(setting.Config.ElasticsearchPort)
	if setting.Config.ElasticsearchUser != "" && setting.Config.ElasticsearchPasswd != "" {
		es.Username = setting.Config.ElasticsearchUser
		es.Password = setting.Config.ElasticsearchPasswd
	}
	if exists, err := es.ExistsIndex("metric", "metric_index", nil); err != nil && err.Error() != "record not found" {
		return err
	} else {
		if !exists {
			_, err = es.CreateIndex("metric")
			if err != nil {
				return err
			}
		}
		esopts := elastigo.MappingOptions{}

		err = es.PutMapping("metric", "metric_index", MetricDefinition{}, esopts)
		if err != nil {
			return err
		}
	}

	return nil
}

var rs *redis.Client

func InitRedis() error {
	opts := &redis.Options{}
	opts.Network = "tcp"
	opts.Addr = setting.Config.RedisAddr
	if setting.Config.RedisPasswd != "" {
		opts.Password = setting.Config.RedisPasswd
	}
	opts.DB = setting.Config.RedisDB
	rs = redis.NewClient(opts)

	return nil
}

// required: name, org_id, target_type, interval, metric, unit

// These validate, and save to elasticsearch

func DefFromJSON(b []byte) (*MetricDefinition, error) {
	def := new(MetricDefinition)
	if err := json.Unmarshal(b, &def); err != nil {
		return nil, err
	}
	def.Id = fmt.Sprintf("%d.%s", def.OrgId, def.Name)
	return def, nil
}

func (m *MetricDefinition) Save() error {
	if err := m.validate(); err != nil {
		return err
	}
	// save in elasticsearch
	return m.indexMetric()
}

func (m *MetricDefinition) validate() error {
	if m.Name == "" || m.OrgId == 0 || (m.TargetType != "derive" && m.TargetType != "gauge") || m.Interval == 0 || m.Metric == "" || m.Unit == "" {
		// TODO: this error message ought to be more informative
		err := log.Printf("Error: metric is not valid!")
		return err
	}
	return nil
}

func (m *MetricDefinition) indexMetric() error {
	log.Printf("indexing %s in elasticsearch\n", m.Id)
	resp, err := es.Index("metric", "metric_index", m.Id, nil, m)
	log.Printf("elasticsearch response: %v\n", resp)
	if err != nil {
		return err
	}
	metricStr, err := json.Marshal(m)
	if err != nil {
		return err
	}
	if rerr := rs.SetEx(m.Id, time.Duration(300)*time.Second, string(metricStr)).Err(); err != nil {
		fmt.Printf("redis err: %s", rerr.Error())
	}
	return nil
}

func GetMetricDefinition(id string) (*MetricDefinition, error) {
	// TODO: fetch from redis before checking elasticsearch
	if v, err := rs.Get(id).Result(); err != nil && err != redis.Nil {
		log.Printf("Error: the redis client bombed: %s", err.Error())
		return nil, err
	} else if err == nil {
		//fmt.Printf("json for %s found in redis\n", id)
		def, err := DefFromJSON([]byte(v))
		if err != nil {
			return nil, err
		}
		return def, nil
	}

	log.Printf("checking elasticsearch for %s\n", id)
	res, err := es.Get("metric", "metric_index", id, nil)
	if err != nil {
		log.Printf("elasticsearch query failed. %s\n", err.Error())
		return nil, err
	}
	//fmt.Printf("elasticsearch query returned %q\n", res.Source)
	//fmt.Printf("placing %s into redis\n", id)
	if rerr := rs.SetEx(id, time.Duration(300)*time.Second, string(*res.Source)).Err(); err != nil {
		log.Printf("redis err: %s", rerr.Error())
	}

	def, err := DefFromJSON(*res.Source)
	if err != nil {
		return nil, err
	}

	return def, nil
}
