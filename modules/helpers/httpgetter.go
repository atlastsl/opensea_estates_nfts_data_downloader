package helpers

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
)

func FetchData(url, apiKey string) (res map[string]any, err error) {

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Add("accept", "application/json")
	if apiKey != "" {
		req.Header.Add("x-api-key", apiKey)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	res = map[string]any{}
	err = json.Unmarshal(body, &res)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func PostData(url, apiKey string, data []byte, respHandler interface{}) error {
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(data))
	if err != nil {
		return err
	}

	req.Header.Add("accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	if apiKey != "" {
		req.Header.Add("x-api-key", apiKey)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	err = json.Unmarshal(body, &respHandler)
	if err != nil {
		return err
	}

	return nil
}
