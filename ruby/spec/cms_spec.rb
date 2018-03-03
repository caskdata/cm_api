# encoding: UTF-8
#
# Copyright © 2017 Cask Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

require_relative 'spec_helper'
require_relative '../lib/endpoints/cms'
require_relative '../lib/endpoints/types'

require 'json'

include ::CmApi::Endpoints::Cms
include ::CmApi::Endpoints::Types

describe CmApi::Endpoints::Cms do

  it 'can get all hosts config' do
    SUMMARY = <<EOF
      {
        "items" : [ {
          "name" : "blacklisted_parcel_products",
          "value" : "foo,bar"
        } ]
      }
EOF

    FULL = <<EOF
      {
        "items" : [ {
          "name" : "blacklisted_parcel_products",
          "value" : "foo,bar",
          "required" : false,
          "default" : "",
          "displayName" : "Blacklisted Products",
          "description" : "Parcels for blacklisted products will not be distributed to the host, nor activated for process execution. Already distributed parcels will be undistributed. Already running process will not be affected until the next restart.",
          "validationState" : "OK"
        }, {
          "name" : "rm_enabled",
          "required" : false,
          "default" : "false",
          "displayName" : "Enable Resource Management",
          "description" : "Enables resource management for all roles on this host.",
          "validationState" : "OK",
          "validationWarningsSuppressed" : false
        } ]
      }
EOF

    resource = MockResource.new
    cms = ClouderaManager.new(resource)

    resource.set_expected(:get, '/cm/allHosts/config', nil, nil, nil, JSON.parse(SUMMARY))
    cfg = cms.get_all_hosts_config
    expect(cfg).to be_an_instance_of Hash
    expect(cfg.length).to eq 1
    expect(cfg['blacklisted_parcel_products']).to eq 'foo,bar'

    resource.set_expected(:get, '/cm/allHosts/config', {'view' => 'full'}, nil, nil, JSON.parse(FULL))
    cfg = cms.get_all_hosts_config('full')
    expect(cfg).to be_an_instance_of Hash
    expect(cfg.length).to eq 2
    expect(cfg['blacklisted_parcel_products']).to be_an_instance_of ApiConfig
    expect(cfg['blacklisted_parcel_products'].required).to eq false
    expect(cfg['rm_enabled'].validationState).to eq 'OK'

    cfg = { 'blacklisted_parcel_products' => 'bar'}
    resource.set_expected(:put, '/cm/allHosts/config', nil, config_to_json(cfg), nil, JSON.parse(SUMMARY))
    cms.update_all_hosts_config(cfg)
  end

  it 'can commission host' do
    resource = MockResource.new
    cms = ClouderaManager.new(resource)

    resource.set_expected(:post, '/cm/commands/hostsDecommission', nil, ['host1', 'host2'], nil, {})
    cms.hosts_decommission(['host1', 'host2'])

    resource.set_expected(:post, '/cm/commands/hostsRecommission', nil, ['host1', 'host2'], nil, {})
    cms.hosts_recommission(['host1', 'host2'])
  end

  it 'can get licensed feature usage' do
    resource = MockResource.new
    cms = ClouderaManager.new(resource)
    json_string = {
      "totals" => {
        "Core" => 8,
        "HBase" => 8,
        "Impala" => 8,
        "Search" => 2,
        "Spark" => 5,
        "Accumulo" => 0,
        "Navigator" => 8
      },
      "clusters" => {
        "Cluster 1" => {
          "Core" => 4,
          "HBase" => 4,
          "Impala" => 4,
          "Search" => 1,
          "Spark" => 1,
          "Accumulo" => 0,
          "Navigator" => 4
        },
        "Cluster 2" => {
          "Core" => 4,
          "HBase" => 4,
          "Impala" => 4,
          "Search" => 1,
          "Spark" => 4,
          "Accumulo" => 0,
          "Navigator" => 4
        }
      }
    }
    resource.set_expected(:get, '/cm/getLicensedFeatureUsage', nil, nil, nil, json_string)
    cms.get_licensed_feature_usage()
  end

  # TODO: test_peer_v10
  # TODO: test_peer_v11

  it 'can import cluster v12' do
    resource = MockResource.new(12)
    cms = ClouderaManager.new(resource)
    data = ApiClusterTemplate.new(resource).to_json_dict
    resource.set_expected(:post, '/cm/importClusterTemplate', {'addRepositories' => true}, data, nil, ApiCommand.new(resource).to_json_dict)
    cms.import_cluster_template(data, true)
  end
end
