package service

import (
	"data-scrubber/biz/errorx"
	"data-scrubber/biz/upstream/tushare"
	logger "github.com/2997215859/golog"
)

var ts *tushare.TuShare

/*
{"request_id":"1cc2e89a-1e9f-4ad4-ae84-ed4390eff364","code":0,"msg":"","data":{"fields":["ts_code","trade_date","pre_close","up_limit","down_limit"],"items":[["000001.SZ","20190625",13.69,15.06,12.32],["000002.SZ","20190625",28.13,30.94,25.32],["000004.SZ","20190625",22.86,25.15,20.57],["000005.SZ","20190625",3.17,3.49,2.85],["000006.SZ","20190625",5.58,6.14,5.02],["000007.SZ","20190625",7.04,7.74,6.34],["000008.SZ","20190625",3.89,4.28,3.5
*/
func InitTuShare() {
	ts = tushare.NewTuShare("32edd62d8ec424bd141e2992ffd0725c51b246e205115188d1576229")
	// 标准用法示例，由请求参数结构体和返回值参数结构体构成，返回值参数结构体通常都有一个all方法来默认获取全部参数
	if ts == nil {
		logger.Fatal("ts is nil")
	}
}

// 20190625
func GetDateLimit(tradeDate string) ([]*PriceLimit, error) {
	r := tushare.QuotationRequest{
		TradeDate: tradeDate,
	}

	rsp, err := ts.StkLimit(r, tushare.StkLimitItems{}.All())
	if err != nil {
		return nil, errorx.NewError("GetDateLimit err: %v", err)
	}
	if rsp.Code != 0 {
		return nil, errorx.NewError("GetDateLimit code != 0. msg = %s", rsp.Code, rsp.Msg)
	}

	res := make([]*PriceLimit, 0)
	for _, v := range rsp.Data.Items {
		res = append(res, &PriceLimit{
			InstrumentId: v[0].(string),
			HighLimit:    v[3].(float64),
			LowLimit:     v[4].(float64),
		})
	}
	return res, nil
}

func GetStockLimit(instrumentId string) (*PriceLimit, error) {
	priceLimit, ok := mapPriceLimit[instrumentId]
	if !ok {
		return nil, errorx.NewError("GetStockLimit(%s) not found", instrumentId)
	}
	return priceLimit, nil
}

type PriceLimit struct {
	InstrumentId string
	HighLimit    float64
	LowLimit     float64
}

var mapPriceLimit map[string]*PriceLimit

func UpdateTuShareDailyLimit(date string) error {
	mapPriceLimit = make(map[string]*PriceLimit)

	priceLimitList, err := GetDateLimit(date)
	if err != nil {
		return errorx.NewError("UpdateTuShareDailyLimit err: %v", err)
	}

	for _, v := range priceLimitList {
		mapPriceLimit[v.InstrumentId] = v
	}
	return nil
}
