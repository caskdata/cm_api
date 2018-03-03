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
require_relative '../lib/endpoints/clusters'
require_relative '../lib/endpoints/types'
#require_relative '../lib/endpoints/services'

include ::CmApi::Endpoints::Clusters
include ::CmApi::Endpoints::Types

describe CmApi::Endpoints::Clusters do

  it 'can add hosts' do
    resource = MockResource.new
    cluster = ApiCluster.new(resource, 'foo')

    data = ApiList.new([ ApiHostRef.new(resource, 'foo') ])

    resource.set_expected(:post, '/clusters/foo/hosts', nil, data, nil, { 'items' => [ { 'hostId' => 'foo' } ] })
    cluster.add_hosts(['foo'])
  end

  it 'can update cdh version' do
    resource = MockResource.new
    cluster = ApiCluster.new(resource, 'foo')

    data = ApiCluster.new(resource, 'foo', nil, '4.2.1')

    resource.set_expected(:put, '/clusters/foo', nil, data, nil, { 'name' => 'foo'})
    cluster.update_cdh_version('4.2.1')
  end

  # TODO: test_upgrade_cdh

  it 'can restart' do
    resource = MockResource.new(5)
    cluster = ApiCluster.new(resource, 'foo')
    resource.set_expected(:post, '/clusters/foo/commands/restart', nil, nil, nil, { 'name' => 'foo'})
    cluster.restart

    resource = MockResource.new(7)
    newCluster = ApiCluster.new(resource, 'bar')
    data = {}
    data['restartOnlyStaleServices'] = false
    data['redeployClientConfiguration'] = true
    resource.set_expected(:post, '/clusters/bar/commands/restart', nil, data, nil, { 'name' => 'bar' })
    newCluster.restart(false, true)

    resource = MockResource.new(11)
    newCluster = ApiCluster.new(resource, 'bar')
    data = {}
    data['restartOnlyStaleServices'] = false
    data['redeployClientConfiguration'] = true
    data['restartServiceNames'] = ['A', 'B']
    resource.set_expected(:post, '/clusters/bar/commands/restart', nil, data, nil, {'name' => 'bar'})
    newCluster.restart(false, true, ['A', 'B'])
  end

  it 'can configure for kerberos' do
    resource = MockResource.new
    cluster = ApiCluster.new(resource, 'foo')

    data = {}
    data['datanodeTransceiverPort'] = 23456
    data['datanodeWebPort'] = 12345

    resource.set_expected(:post, '/clusters/foo/commands/configureForKerberos', nil, data, nil, {'name' => 'foo'})
    cluster.configure_for_kerberos(23456, 12345)
  end

  it 'can export cluster template' do
    resource = MockResource.new
    cluster = ApiCluster.new(resource, 'foo')
    resource.set_expected(:get, '/clusters/foo/export', {'exportAutoConfig' => true}, nil, nil, ApiClusterTemplate.new(resource).to_json_dict())
    cluster.export(true)
  end

  it 'can refresh pools' do
    resource = MockResource.new
    cluster = ApiCluster.new(resource, 'foo')

    resource.set_expected(:post, '/clusters/foo/commands/poolsRefresh', nil, nil, nil, {'name' => 'foo'})
    cluster.pools_refresh
  end

  it 'can list dfs services' do
    resource = MockResource.new
    cluster = ApiCluster.new(resource, 'foo')
    data = nil 
    resource.set_expected(:get, '/clusters/foo/dfsServices', nil, data, nil, { 'name' => 'foo' })
    cluster.list_dfs_services()

    data = nil
    resource.set_expected(:get, '/clusters/foo/dfsServices?view=EXPORT', nil, data, nil, {'name' => 'foo'})
    cluster.list_dfs_services('EXPORT')
  end


#def set_expected(method, reqpath, params = nil, data = nil, headers = nil, retdata = nil)

end
