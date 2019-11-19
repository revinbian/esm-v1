/*
Copyright 2016 Medcl (m AT medcl.net)

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

package main

import (
        "bytes"
        "encoding/json"
        "errors"
        "fmt"
        "io"
        "io/ioutil"
        log "github.com/cihub/seelog"
        "regexp"
        "strings"
)

type ESAPIV0 struct {
        Host      string //eg: http://localhost:9200
        Auth      *Auth  //eg: user:pass
        HttpProxy string //eg: http://proxyIp:proxyPort
}

/*
* 获取集群健康状态
*/
func (s *ESAPIV0) ClusterHealth() *ClusterHealth {

        url := fmt.Sprintf("%s/_cluster/health", s.Host)
        r, body, errs := Get(url, s.Auth,s.HttpProxy)

        if r!=nil && r.Body!=nil {
                // ioutil.Discard 类似 /dev/null 设备，是个无底洞，啥都可以往里扔
                // 可以理解为把r.body置为空。因为上面已经拿到了body
                io.Copy(ioutil.Discard, r.Body)  
                defer r.Body.Close()
        }

        if errs != nil {
                return &ClusterHealth{Name: s.Host, Status: "unreachable"}
        }

        log.Debug(url)
        log.Debug(body)

        health := &ClusterHealth{} // 实例一个空的结构体对象
        err := json.Unmarshal([]byte(body), health) // 解析json.Unmarshal固定用法,参数1.Unmarshal的第一个参数是json字符串，第二个参数是接受json解析的数据结构(必须是指针类型)。

        if err != nil {
                log.Error(body)
                return &ClusterHealth{Name: s.Host, Status: "unreachable"}
        }
        return health
}

/**
* es的Bulk操作
参数: data
*/
func (s *ESAPIV0) Bulk(data *bytes.Buffer) {
        if data == nil || data.Len() == 0 {
                log.Error("data is empty, skip")
                return
        }
        data.WriteRune('\n') // 末尾写入
        url := fmt.Sprintf("%s/_bulk", s.Host)

        log.Trace(url)
        log.Trace(data.String())

        body,err:=Request("POST",url,s.Auth,data,s.HttpProxy)

        if err != nil {
                fmt.Println(err)
                log.Error(err)
                return
        }
        //log.Trace(url,string(body))
        //fmt.Println(string(body))
        response:=BulkResponse{} // 实例bulk的返回结构体
        json.Unmarshal([]byte(body), &response) //  json解析返回的bulk
        //v,_:=json.MarshalIndent(&response,""," ")
        //fmt.Println(string(v))
        if err == nil {
        //        fmt.Println(response)
                if response.Errors{
                        fmt.Println(body)
                }
        }

        data.Reset()  // 清空数据
}

/**
* 获取索引设置
* 参数: indexNames 索引名字 (支持多个,逗号隔开)
*/
func (s *ESAPIV0) GetIndexSettings(indexNames string) (*Indexes, error) {

        // get all settings
        allSettings := &Indexes{} //创建Indexes结构体指针类型

        url := fmt.Sprintf("%s/%s/_settings", s.Host, indexNames)
        resp, body, errs := Get(url, s.Auth,s.HttpProxy)

        if resp!=nil && resp.Body!=nil {
                io.Copy(ioutil.Discard, resp.Body) // 置为空
                defer resp.Body.Close() //defer延迟执行,函数执行完,关闭
        }

        if errs != nil {
                return nil, errs[0]
        }

        if resp.StatusCode != 200 {
                return nil, errors.New(body)
        }

        log.Debug(body)

        err := json.Unmarshal([]byte(body), allSettings)
        if err != nil {
                panic(err)
                return nil, err
        }

        return allSettings, nil
}

/**
* 获取索引的mapping配置信息
* 参考: copyAllIndexes 
* 参数: indexNames 索引名字 (支持多个,逗号隔开)
*/
func (s *ESAPIV0) GetIndexMappings(copyAllIndexes bool, indexNames string) (string, int, *Indexes, error) {
        url := fmt.Sprintf("%s/%s/_mapping", s.Host, indexNames)
        resp, body, errs := Get(url, s.Auth,s.HttpProxy)

        if resp!=nil&& resp.Body!=nil{
                io.Copy(ioutil.Discard, resp.Body)
                defer resp.Body.Close()
        }

        if errs != nil {
                log.Error(errs)
                return "", 0, nil, errs[0]
        }


        if resp.StatusCode != 200 {
                return "", 0, nil, errors.New(body)
        }

        idxs := Indexes{}
        er := json.Unmarshal([]byte(body), &idxs)

        if er != nil {
                log.Error(body)
                return "", 0, nil, er
        }

        // remove indexes that start with . if user asked for it
        //if copyAllIndexes == false {
        //      for name := range idxs {
        //              switch name[0] {
        //              case '.':
        //                      delete(idxs, name)
        //              case '_':
        //                      delete(idxs, name)
        //
//
//                      }
//              }
//      }

        // if _all indexes limit the list of indexes to only these that we kept
        // after looking at mappings
        if indexNames == "_all" {   // 匹配所有

                var newIndexes []string
                for name := range idxs {
                        newIndexes = append(newIndexes, name)
                }
                indexNames = strings.Join(newIndexes, ",")

        } else if strings.Contains(indexNames, "*") || strings.Contains(indexNames, "?") { // 正则匹配的方式

                r, _ := regexp.Compile(indexNames) // 解析看看正则表达式是否合法

                //check index patterns
                var newIndexes []string
                for name := range idxs {
                        matched := r.MatchString(name)
                        if matched {
                                newIndexes = append(newIndexes, name)
                        }
                }
                indexNames = strings.Join(newIndexes, ",")

        }

        i := 0
        // wrap in mappings if moving from super old es
        for name, idx := range idxs {
                i++
                if _, ok := idx.(map[string]interface{})["mappings"]; !ok {  // ???
                        (idxs)[name] = map[string]interface{}{
                                "mappings": idx,
                        }
                }
        }

        return indexNames, i, &idxs, nil
}

/**
* 获取一个空的索引配置
* 返回:map类型
*/
func getEmptyIndexSettings() map[string]interface{} {
        tempIndexSettings := map[string]interface{}{}
        tempIndexSettings["settings"] = map[string]interface{}{}
        tempIndexSettings["settings"].(map[string]interface{})["index"] = map[string]interface{}{}
        return tempIndexSettings
}

/**
* 清除所有的setting
* 无返回值
*/
func cleanSettings(settings map[string]interface{}) {
        //clean up settings
        delete(settings["settings"].(map[string]interface{})["index"].(map[string]interface{}), "creation_date")
        delete(settings["settings"].(map[string]interface{})["index"].(map[string]interface{}), "uuid")
        delete(settings["settings"].(map[string]interface{})["index"].(map[string]interface{}), "version")
        delete(settings["settings"].(map[string]interface{})["index"].(map[string]interface{}), "provided_name")
}

/**
* 更新索引的setting信息
*/
func (s *ESAPIV0) UpdateIndexSettings(name string, settings map[string]interface{}) error {

        log.Debug("update index: ", name, settings)
        cleanSettings(settings)
        url := fmt.Sprintf("%s/%s/_settings", s.Host, name)

        if _, ok := settings["settings"].(map[string]interface{})["index"]; ok {
                if set, ok := settings["settings"].(map[string]interface{})["index"].(map[string]interface{})["analysis"]; ok {
                        log.Debug("update static index settings: ", name)
                        staticIndexSettings := getEmptyIndexSettings()
                        staticIndexSettings["settings"].(map[string]interface{})["index"].(map[string]interface{})["analysis"] = set
                        Post(fmt.Sprintf("%s/%s/_close", s.Host, name), s.Auth, "",s.HttpProxy)
                        body := bytes.Buffer{}
                        enc := json.NewEncoder(&body)
                        enc.Encode(staticIndexSettings)
                        bodyStr, err := Request("PUT", url, s.Auth, &body,s.HttpProxy)
                        if err != nil {
                                log.Error(bodyStr, err)
                                panic(err)
                                return err
                        }
                        delete(settings["settings"].(map[string]interface{})["index"].(map[string]interface{}), "analysis")
                        Post(fmt.Sprintf("%s/%s/_open", s.Host, name), s.Auth, "",s.HttpProxy)
                }
        }

        log.Debug("update dynamic index settings: ", name)

        body := bytes.Buffer{}
        enc := json.NewEncoder(&body)
        enc.Encode(settings)
        _, err := Request("PUT", url, s.Auth, &body,s.HttpProxy)

        return err
}

/**
* 更新索引的mapping信息
*/
func (s *ESAPIV0) UpdateIndexMapping(indexName string, settings map[string]interface{}) error {

        log.Debug("start update mapping: ", indexName,settings)

        for name, mapping := range settings {

                log.Debug("start update mapping: ", indexName,name,mapping)

                url := fmt.Sprintf("%s/%s/%s/_mapping", s.Host, indexName, name)

                body := bytes.Buffer{}
                enc := json.NewEncoder(&body)
                enc.Encode(mapping)
                res, err := Request("POST", url, s.Auth, &body,s.HttpProxy)
                if(err!=nil){
                        log.Error(url)
                        log.Error(body.String())
                        log.Error(err,res)
                        panic(err)
                }
        }
        return nil
}

/**
* 删除索引
*/
func (s *ESAPIV0) DeleteIndex(name string) (err error) {

        log.Debug("start delete index: ", name)

        url := fmt.Sprintf("%s/%s", s.Host, name)

        Request("DELETE", url, s.Auth, nil,s.HttpProxy)

        log.Debug("delete index: ", name)

        return nil
}

/**
* 创建索引
*/
func (s *ESAPIV0) CreateIndex(name string, settings map[string]interface{}) (err error) {
        cleanSettings(settings)

        body := bytes.Buffer{}
        enc := json.NewEncoder(&body)
        enc.Encode(settings)
        log.Debug("start create index: ", name, settings)

        url := fmt.Sprintf("%s/%s", s.Host, name)

        resp, err := Request("PUT", url, s.Auth, &body,s.HttpProxy)
        log.Debugf("response: %s",resp)

        return err
}

/**
* 刷新索引
*/
func (s *ESAPIV0) Refresh(name string) (err error) {


        log.Debug("refresh index: ", name)

        url := fmt.Sprintf("%s/%s/_refresh", s.Host, name)

        resp,_,_:=Post(url,s.Auth,"",s.HttpProxy)
        if resp!=nil&& resp.Body!=nil{
                io.Copy(ioutil.Discard, resp.Body)
                defer resp.Body.Close()
        }

        return nil
}

/**
* scroll 首次请求
* 参数: indexNames 索引名(支持多个)
* 参数: scrollTime 超时时间
* 参数: docBufferCount 请求页面大小
* 参数: query  body查询体
* 参数: slicedId 没有用到
* 参数: maxSlicedCount 没有用到
* 参数: fields
*/
func (s *ESAPIV0) NewScroll(indexNames string, scrollTime string, docBufferCount int,query string, slicedId,maxSlicedCount int, fields string) (scroll interface{}, err error) {

        // curl -XGET 'http://es-0.9:9200/_search?search_type=scan&scroll=10m&size=50'
        url := fmt.Sprintf("%s/%s/_search?search_type=scan&scroll=%s&size=%d", s.Host, indexNames, scrollTime, docBufferCount)

        jsonBody:=""
        if len(query) > 0 || len(fields) > 0 {
                queryBody := map[string]interface{}{}
                if len(fields) > 0 {
                        if !strings.Contains(fields, ",") {
                                log.Error("The fields shoud be seraprated by ,")
                                return
                        } else {
                                queryBody["_source"] = strings.Split(fields, ",")
                        }
                }

                if len(query) > 0 {
                        queryBody["query"] = map[string]interface{}{}
                        queryBody["query"].(map[string]interface{})["query_string"] = map[string]interface{}{}
                        queryBody["query"].(map[string]interface{})["query_string"].(map[string]interface{})["query"] = query

                        jsonArray, err := json.Marshal(queryBody) // 将数据编码成json字符串
                        if err != nil {
                                log.Error(err)

                        } else {
                                jsonBody = string(jsonArray) // 强转
                        }
                }

        }
        resp, body, errs := Post(url, s.Auth,jsonBody,s.HttpProxy)

        if resp!=nil&& resp.Body!=nil{
                io.Copy(ioutil.Discard, resp.Body)
                defer resp.Body.Close()
        }

        if err != nil {
                log.Error(errs)
                return nil, errs[0]
        }

        log.Trace("new scroll,",url, body)

        if err != nil {
                log.Error(err)
                return nil, err
        }

        if resp.StatusCode != 200 {
                return nil, errors.New(body)
        }

        scroll = &Scroll{} // 实例Scroll结构体指针类型
        err = json.Unmarshal([]byte(body), scroll) // 解析
        if err != nil {
                log.Error(err)
                return nil, err
        }

        return scroll, err
}

/*
* 根据　scrollId　获取数据
* 参数: scrollTime 超时时间
* 参数: scrollId scrollId
*/
func (s *ESAPIV0) NextScroll(scrollTime string, scrollId string) (interface{}, error) {
        //  curl -XGET 'http://es-0.9:9200/_search/scroll?scroll=5m'
        id := bytes.NewBufferString(scrollId)
        url := fmt.Sprintf("%s/_search/scroll?scroll=%s&scroll_id=%s", s.Host, scrollTime, id)
        resp, body, errs := Get(url, s.Auth, s.HttpProxy)

        if resp!=nil && resp.Body!=nil {
                io.Copy(ioutil.Discard, resp.Body)
                defer resp.Body.Close()
        }

        if errs != nil {
                log.Error(errs)
                return nil, errs[0]
        }

        if resp.StatusCode != 200 {
                return nil, errors.New(body)
        }

        log.Trace("next scroll,",url,body)

        // decode elasticsearch scroll response
        scroll := &Scroll{}
        err := json.Unmarshal([]byte(body), &scroll)
        if err != nil {
                log.Error(body)
                log.Error(err)
                return nil, err
        }

        return scroll, nil
}
