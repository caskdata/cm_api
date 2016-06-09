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

require 'date'

require_relative 'types'

module CmApi
  module Endpoints
    module Hosts

      include ::CmApi::Endpoints::Types

      HOSTS_PATH = '/hosts'

      def create_host(host_id, name, ipaddr, rack_id = nil)
        apihost = ApiHost.new(self, host_id, name, ipaddr, rack_id)
        return call(:post, HOSTS_PATH, ApiHost, true, [apihost])[0]
      end

      def get_host(host_id)
        return call(:get, "#{HOST_PATH}/#{host_id}", ApiHost)
      end

      def get_all_hosts(view = nil)
        return call(:get, HOSTS_PATH, ApiHost, true, nil, view && { 'view' => view } || nil)
      end

      def delete_host(host_id)
        return call(:delete, "#{HOSTS_PATH}/#{host_id}", ApiHost)
      end

      class ApiHost < ::CmApi::Endpoints::Types::BaseApiResource
        @_ATTRIBUTES = {
          'hostId' => nil,
          'hostname' => nil,
          'ipAddress' => nil,
          'rackId' => nil,
          'status' => ::CmApi::Endpoints::Types::ROAttr.new,
          'lastHeartbeat' => ::CmApi::Endpoints::Types::ROAttr.new(DateTime),
          'roleRefs' => ::CmApi::Endpoints::Types::ROAttr.new(::CmApi::Endpoints::Types::ApiRoleRef),
          'healthSummary' => ::CmApi::Endpoints::Types::ROAttr.new,
          'healthChecks' => ::CmApi::Endpoints::Types::ROAttr.new,
          'hostUrl' => ::CmApi::Endpoints::Types::ROAttr.new,
          'commissionState' => ::CmApi::Endpoints::Types::ROAttr.new,
          'maintenanceMode' => ::CmApi::Endpoints::Types::ROAttr.new,
          'maintenanceOwners' => ::CmApi::Endpoints::Types::ROAttr.new,
          'numCores' => ::CmApi::Endpoints::Types::ROAttr.new,
          'numPhysicalCores' => ::CmApi::Endpoints::Types::ROAttr.new,
          'totalPhysMemBytes' => ::CmApi::Endpoints::Types::ROAttr.new
        }

        def initialize(resource_root, hostId = nil, hostname = nil, ipAddress = nil, rackId = nil)
          super(resource_root, {:hostId => hostId, :hostname => hostname, :ipAddress => ipAddress, :rackId => rackId})
        end

        def to_s()
          return "<ApiHost>: #{@hostId} (#{@ipAddress})"
        end

        def _path()
          return "#{HOSTS_PATH}/#{@hostId}"
        end

        def _put_host()
          return _put('', ApiHost, false, self)
        end

        def get_config(view = nil)
          return _get_config('config', view)
        end

        def update_config(config)
          return _update_config('config', config)
        end




        def set_rack_id(rackId)
          @rackId = rackId
          _put_host()
        end

      end 
    end
  end
end
