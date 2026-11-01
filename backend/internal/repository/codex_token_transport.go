package repository

import (
	"github.com/imroc/req/v3"
	"time"
)

func newCodexTokenReqClient(proxyURL string) (*req.Client, error) {
	return newCodexDeviceReqClientWithAuthCA(proxyURL, 120*time.Second, true)
}
