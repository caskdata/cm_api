#!/usr/bin/env ruby
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
require_relative 'endpoints/hosts.rb'
require_relative 'endpoints/cms.rb'

module CmApi
  API_AUTH_REALM = 'Cloudera Manager'.freeze
  API_CURRENT_VERSION = 12

  # Any error result from the API is converted into this exception type.
  # This handles errors from the HTTP level as well as the API level.
  class ApiException < RestException
    def initialize(error)
      # The parent class will set up @code and @message
      super
      begin
        # See if the body is json
        json_body = JSON.parse(@message)
        @message = json_body['message']
      rescue JSON::ParserError
        return
      end
    end
  end

  # Resource object that provides methods for managing the top-level API resources.
  class ApiResource < Resource
    include ::CmApi::Endpoints::Clusters
    include ::CmApi::Endpoints::Hosts
    #include ::CmApi::Endpoints::Services
    include ::CmApi::Endpoints::Roles
    #include ::CmApi::Endpoints::Services

    attr_accessor :version

    # Creates a Resource object that provides API endpoints
    #   @param server_host: The hostname of the Cloudera Manager server.
    #   @param server_port: The port of the server. Defaults to 7180 (http) or
    #    7183 (https).
    #   @param username: Login name.
    #   @param password: Login password.
    #   @param use_tls: Whether to use tls (https).
    #   @param version: API version.
    #   @return: Resource object referring to the root.
    def initialize(server_host, server_port = nil, username = 'admin', password = 'admin', use_tls = false, version = API_CURRENT_VERSION)
      @version = version
      protocol = use_tls ? 'https' : 'http'
      server_port = use_tls ? 7183 : 7180 if server_port.nil?
      base_url = "#{protocol}://#{server_host}:#{server_port}/api/v#{version}"

      client = HttpClient.new(base_url, ApiException)
      client.set_basic_auth(username, password, API_AUTH_REALM)
      client.headers = { :'content-type' => 'application/json' }
      super(client)
    end
  end

  def self.get_root_resource(server_host, server_port = nil, username = 'admin', password = 'admin', use_tls = false, version = API_CURRENT_VERSION)
    ApiResource.new(server_host, server_port, username, password, use_tls, version)
  end
end
