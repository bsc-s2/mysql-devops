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

	for i, v := range a {

		if i >= lenB {
			return 1, nil
		}

		var rst int

		switch v.(type) {
		case int, int8, int16, int32, int64:
			ai, _ := interfaceToInt64(v)
			bi, _ := interfaceToInt64(b[i])
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

	if lenA < lenB {
		return -1, nil
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

func interfaceToInt64(v interface{}) (int64, error) {
	switch v.(type) {
	case int:
		return int64(v.(int)), nil
	case int8:
		return int64(v.(int8)), nil
	case int16:
		return int64(v.(int16)), nil
	case int32:
		return int64(v.(int32)), nil
	case int64:
		return v.(int64), nil
	default:
		return 0, errors.New(fmt.Sprintf("unknow type of value:%v, type: %T", v, v))
	}
}

func unmarshalYAML(filename string, v interface{}) error {
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

func marshalYAML(v interface{}) (string, error) {

	data, err := yaml.Marshal(v)
	if err != nil {
		return "", err
	}

	return string(data), nil
}
