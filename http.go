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
	"net/http" //实现了http客户端与服务端的实现
	"github.com/parnurzeal/gorequest"　// 简化的HTTP客户端(灵感来自SuperAgent)
	log "github.com/cihub/seelog" // 本地日志库
	"io/ioutil" // 实现了一些 I/O 实用程序功能
	"io"// 操作系统相关
	"errors"
	"bytes" // 包定义了一些操作 byte slice 的便利操作
	"net/url" // url相关操作
	"crypto/tls" //tls协议相关
)

/**
* 普通函数
* 发送get请求
*/
func Get(url string,auth *Auth,proxy string) (*http.Response, string, []error) {
	request := gorequest.New()

	tr := &http.Transport{　// 设置传输配置
		DisableKeepAlives: true,
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	request.Transport=tr


	if(auth!=nil){ // 设置basic验证
		request.SetBasicAuth(auth.User,auth.Pass)
	}

	request.Header["Content-Type"]= "application/json"　// 设置请求头格式
	//request.Header.Set("Content-Type", "application/json")

	if(len(proxy)>0){　// 设置代理
		request.Proxy(proxy)
	}

	resp, body, errs := request.Get(url).End()　// 发送get请求，获取返回结果
	return resp, body, errs

}

/**
* 普通函数
* 发送post请求
*/
func Post(url string,auth *Auth, body string,proxy string)(*http.Response, string, []error)  {
	request := gorequest.New()
	tr := &http.Transport{　// 设置传输配置
		DisableKeepAlives: true,
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	request.Transport=tr

	if(auth!=nil){　// 设置basic验证
		request.SetBasicAuth(auth.User,auth.Pass)
	}

	request.Header["Content-Type"]="application/json"　// 设置请求头格式
	
	if(len(proxy)>0){ // 设置代理
		request.Proxy(proxy)
	}

	request.Post(url) // 发送post请求，获取返回结果

	if(len(body)>0) { //设置body体
		request.Send(body)
	}

	return request.End()　// 返回post请求结果
}

/**
* 普通函数
* 发送delete请求
*/
func newDeleteRequest(client *http.Client,method, urlStr string) (*http.Request, error) {
	if method == "" {　// 设置默认的请求方式
		// We document that "" means "GET" for Request.Method, and people have
		// relied on that from NewRequest, so keep that working.
		// We still enforce validMethod for non-empty methods.
		method = "GET"
	}
	u, err := url.Parse(urlStr) // 解析验证是否合法
	if err != nil {
		return nil, err
	}

	req := &http.Request{　// 发送请求，使用是http包
		Method:     method,
		URL:        u,
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header:     make(http.Header),
		Host:       u.Host,
	}
	return req, nil
}

/**
* 普通函数
* 通用请求方法
*/
func Request(method string,r string,auth *Auth,body *bytes.Buffer,proxy string)(string,error)  {

	//TODO use global client
	var client *http.Client
	client = &http.Client{}
	if(len(proxy)>0){
		proxyURL, err := url.Parse(proxy)
		if(err!=nil){
			log.Error(err)
		}else{
			transport := &http.Transport{
				Proxy: http.ProxyURL(proxyURL),
				DisableKeepAlives: true,
			}
			client = &http.Client{Transport: transport}
		}
	}

	tr := &http.Transport{
		DisableKeepAlives: true,
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
	}

	client.Transport=tr

	var reqest *http.Request
	if(body!=nil){
		reqest, _ =http.NewRequest(method,r,body)
	}else{
		reqest, _ = newDeleteRequest(client,method,r)
	}
	if(auth!=nil){
		reqest.SetBasicAuth(auth.User,auth.Pass)
	}

	reqest.Header.Set("Content-Type", "application/json")


	resp,errs := client.Do(reqest)
	if errs != nil {
		log.Error(errs)
		return "",errs
	}

	if resp!=nil&& resp.Body!=nil{
		//io.Copy(ioutil.Discard, resp.Body)
		defer resp.Body.Close()
	}

	if resp.StatusCode != 200 {
		b, _ := ioutil.ReadAll(resp.Body)　//  读取直到 EOF
		return "",errors.New("server error: "+string(b))
	}

	respBody,err:=ioutil.ReadAll(resp.Body)　　//  读取直到 EOF

	if err != nil {
		log.Error(err)
		return string(respBody),err
	}

	log.Trace(r,string(respBody))

	if err != nil {
		return string(respBody),err
	}
	io.Copy(ioutil.Discard, resp.Body) //将resp中的body置为空，上面已经获取到了
	defer resp.Body.Close() //函数退出，关闭请求body,收尾工作
	return string(respBody),nil　// 强转
}
