/*
Copyright The Kubernetes Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package driver

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	bolt "go.etcd.io/bbolt"
	berrors "go.etcd.io/bbolt/errors"
	"k8s.io/apimachinery/pkg/types"
)

var (
	secondaryNICPodConfigsBucket = []byte("secondary_nic_pod_configs")
	secondaryNICDevicesBucket    = []byte("device_configs")
)

type secondaryNICBoltCheckpointer struct {
	db *bolt.DB
}

var _ SecondaryNICCheckpointer = &secondaryNICBoltCheckpointer{}

func newSecondaryNICBoltCheckpointer(path string) (*secondaryNICBoltCheckpointer, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0750); err != nil {
		return nil, fmt.Errorf("create secondary NIC config db directory: %w", err)
	}
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, fmt.Errorf("open secondary NIC config db: %w", err)
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(secondaryNICPodConfigsBucket)
		return err
	}); err != nil {
		db.Close()
		return nil, fmt.Errorf("initialize secondary NIC config db bucket: %w", err)
	}
	return &secondaryNICBoltCheckpointer{db: db}, nil
}

func (c *secondaryNICBoltCheckpointer) Close() error {
	return c.db.Close()
}

func (c *secondaryNICBoltCheckpointer) Store(podUID types.UID, deviceName string, config SecondaryNICPodConfig) error {
	data, err := json.Marshal(config)
	if err != nil {
		return err
	}
	return c.db.Update(func(tx *bolt.Tx) error {
		root := tx.Bucket(secondaryNICPodConfigsBucket)
		if root == nil {
			return berrors.ErrBucketNotFound
		}
		podBucket, err := root.CreateBucketIfNotExists([]byte(podUID))
		if err != nil {
			return err
		}
		devices, err := podBucket.CreateBucketIfNotExists(secondaryNICDevicesBucket)
		if err != nil {
			return err
		}
		return devices.Put([]byte(deviceName), data)
	})
}

func (c *secondaryNICBoltCheckpointer) GetOrCreate() (map[types.UID]map[string]SecondaryNICPodConfig, error) {
	result := make(map[types.UID]map[string]SecondaryNICPodConfig)
	err := c.db.View(func(tx *bolt.Tx) error {
		root := tx.Bucket(secondaryNICPodConfigsBucket)
		if root == nil {
			return nil
		}
		return root.ForEach(func(podUID, value []byte) error {
			if value != nil {
				return nil
			}
			podBucket := root.Bucket(podUID)
			if podBucket == nil {
				return nil
			}
			devices := podBucket.Bucket(secondaryNICDevicesBucket)
			if devices == nil {
				return nil
			}
			configs := make(map[string]SecondaryNICPodConfig)
			if err := devices.ForEach(func(deviceName, data []byte) error {
				if data == nil {
					return nil
				}
				var config SecondaryNICPodConfig
				if err := json.Unmarshal(data, &config); err != nil {
					return fmt.Errorf("corrupted secondary NIC config for pod %s device %s: %w", podUID, deviceName, err)
				}
				configs[string(deviceName)] = config
				return nil
			}); err != nil {
				return err
			}
			if len(configs) > 0 {
				result[types.UID(string(podUID))] = configs
			}
			return nil
		})
	})
	if err != nil {
		return nil, fmt.Errorf("read secondary NIC config checkpoint: %w", err)
	}
	return result, nil
}

func (c *secondaryNICBoltCheckpointer) DeletePods(podUIDs []types.UID) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		root := tx.Bucket(secondaryNICPodConfigsBucket)
		if root == nil {
			return nil
		}
		for _, podUID := range podUIDs {
			if err := root.DeleteBucket([]byte(podUID)); err != nil && err != berrors.ErrBucketNotFound {
				return err
			}
		}
		return nil
	})
}
