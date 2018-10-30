package main

import (
	"errors"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"reflect"
	"strings"

	yaml "gopkg.in/yaml.v2"
)

func hashStringSliceToInt32(src []string) (int64, error) {
	h := fnv.New32()
	for _, s := range src {
		_, err := h.Write([]byte(s))
		if err != nil {
			return 0, err
		}
	}

	return int64(h.Sum32()), nil
}

func compareSlice(a, b []interface{}) (int, error) {
	lenA := len(a)
	lenB := len(b)
	if lenA != lenB {
		return 0, errors.New(fmt.Sprintf("length of slices not equals: %d != %d", lenA, lenB))
	}

	for i, v := range a {
		var rst int
		switch v.(type) {
		case int:
			ai := reflect.ValueOf(v).Int()
			bi := reflect.ValueOf(b[i]).Int()
			rst = compareInt(ai, bi)
		case string:
			rst = strings.Compare(v.(string), b[i].(string))
		default:
			return 0, errors.New(fmt.Sprintf("unknow type of element: %v", reflect.TypeOf(v)))
		}

		if rst == 0 {
			continue
		}

		return rst, nil
	}

	return 0, nil
}

func compareInt(a, b int64) int {
	if a == b {
		return 0
	}

	if a > b {
		return 1
	} else {
		return -1
	}
}

func parseYAML(filename string, v interface{}) error {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return err
	}

	err = yaml.Unmarshal(data, v)
	if err != nil {
		return err
	}

	return nil
}

func dumpYAML(v interface{}) (string, error) {

	data, err := yaml.Marshal(v)
	if err != nil {
		return "", err
	}

	return string(data), nil
}
