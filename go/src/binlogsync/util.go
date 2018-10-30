package main

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"strconv"
	"strings"
)

func calcHashToInt64(src []byte) (int64, error) {
	h := sha256.New()
	h.Write(src)

	hex_str := fmt.Sprintf("%x", h.Sum(nil))

	num, err := strconv.ParseInt(hex_str[:8], 16, 64)
	if err != nil {
		return 0, err
	}
	return num, nil
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
