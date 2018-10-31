package main

import (
	"encoding/json"
	"errors"
	"hash/fnv"
	"io/ioutil"
	"strings"
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

func compareStringSlice(a, b []string) (int, error) {
	if len(a) != len(b) {
		return 0, errors.New("length of slices not equals")
	}

	for i, v := range a {
		rst := strings.Compare(v, b[i])
		if rst == 0 {
			continue
		}
		return rst, nil
	}

	return 0, nil
}

type JsonStruct struct {
}

func NewJsonStruct() *JsonStruct {
	return &JsonStruct{}
}

func (jst *JsonStruct) Load(filename string, v interface{}) error {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return err
	}

	err = json.Unmarshal(data, v)
	if err != nil {
		return err
	}

	return nil
}
