##!/usr/bin/env ruby
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

require 'json'
require_relative 'http_client'
require_relative 'resource'
require_relative 'endpoints/types.rb'
require_relative 'endpoints/clusters.rb'


module CmApi
  #include ::CmApi::Endpoints::Clusters

  API_AUTH_REALM = 'Cloudera Manager'
  API_CURRENT_VERSION = 10

  class ApiException < RestException
    def initialize(error)
      super
      begin
        json_body = JSON.parse(@message)
        @message = json_body['message']
      rescue
        # ignore json parsing error
      end
    end
  end

  class ApiResource < Resource
    include ::CmApi::Endpoints::Clusters
    attr_accessor :version
    def initialize(server_host, server_port = nil, username = 'admin', password = 'admin', use_tls = false, version = API_CURRENT_VERSION)
      @version = version
      protocol = use_tls ? 'https' : 'http'
      if server_port.nil?
        server_port = use_tls ? 7183 : 7180
      end
      base_url = "#{protocol}://#{server_host}:#{server_port}/api/v#{version}"

      client = HttpClient.new(base_url, exc_class=ApiException) 
      client.set_basic_auth(username, password, API_AUTH_REALM)
      client.headers = { :'content-type' => 'application/json' }
      super(client)
    end
  end

  def self.get_root_resource(server_host, server_port = nil, username = 'admin', password = 'admin', use_tls = false, version = API_CURRENT_VERSION)
    ApiResource.new(server_host, server_port, username, password, use_tls, version)
  end
end
