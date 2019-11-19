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

import "bytes"

/**
* esb api接口
*/
type ESAPI interface{
	ClusterHealth() *ClusterHealth 　// 健康检查
	Bulk(data *bytes.Buffer)　// bulk操作
	GetIndexSettings(indexNames string) (*Indexes, error)　// 获取索引配置
	DeleteIndex(name string) (error)　// 删除索引
	CreateIndex(name string,settings map[string]interface{}) (error)　// 创建索引
	GetIndexMappings(copyAllIndexes bool,indexNames string)(string,int,*Indexes,error) // 获取索引的映射配置
	UpdateIndexSettings(indexName string,settings map[string]interface{})(error) // 更新索引配置
	UpdateIndexMapping(indexName string,mappings map[string]interface{})(error) //　更新索引映射配置
	NewScroll(indexNames string,scrollTime string,docBufferCount int,query string, slicedId,maxSlicedCount int, fields string)(interface{}, error)　// 第一次scoll请求
	NextScroll(scrollTime string,scrollId string)(interface{},error)　//　根据scrollId获取下一次的scroll
	Refresh(name string) (err error) // 刷新索引
}
