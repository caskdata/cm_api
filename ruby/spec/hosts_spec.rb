# #!/usr/bin/env ruby
# encoding: UTF-8
#
# Copyright © 2016 Cask Data, Inc.
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
require_relative '../lib/endpoints/types'
require_relative '../lib/endpoints/hosts'

include ::CmApi::Endpoints::Types
include ::CmApi::Endpoints::Hosts

describe CmApi::Endpoints::Hosts do
  it 'can set rack id' do
    resource = MockResource.new
    host = ApiHost.new(resource, 'fooId', 'foo', '1.2.3.4', '/foo')
    data = ApiHost.new(resource, 'fooId', 'foo', '1.2.3.4', '/bar')

    resource.set_expected(:put, '/hosts/fooId', nil, data, nil, 'rackId' => '/bar')
    host.set_rack_id('/bar')
  end
end
