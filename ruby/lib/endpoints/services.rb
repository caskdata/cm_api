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

# require 'date'

require_relative 'types'

module CmApi
  module Endpoints
    # Module for services-related methods and types
    module Services
      include ::CmApi::Endpoints::Types
      # include ::CmApi::Endpoints::Roles

      SERVICES_PATH = '/clusters/%s/services'.freeze
      SERVICE_PATH = '/clusters/%s/services/%s'.freeze
      ROLETYPES_CFG_KEY = 'roleTypeConfigs'.freeze

      def create_service(name, service_type, cluster_name = 'default')
        apiservice = ApiService.new(self, name, service_type)
        call_resource(method(:post), format(SERVICES_PATH, cluster_name), ApiService, true, [apiservice])[0]
      end

      def get_service(name, cluster_name = 'default')
        _get_service(self, format('%s/%s', format(SERVICES_PATH, cluster_name), name))
      end

      def _get_service(path)
        call_resource(method(:get), path, ApiService)
      end

      def get_all_services(cluster_name = 'default', view = nil)
        call_resource(method(:get), format(SERVICES_PATH, cluster_name), ApiService, true, nil, view && { 'view' => view } || nil)
      end

      def delete_service(name, cluster_name = 'default')
        call_resource(method(:delete), format('%s/%s', format(SERVICES_PATH, cluster_name), name), ApiService)
      end

      # Model for a service
      class ApiService < BaseApiResource
        @_ATTRIBUTES = {
          'name' => nil,
          'type' => nil,
          'displayName' => nil,
          'serviceState' => ROAttr.new,
          'healthSummary' => ROAttr.new,
          'healthChecks' => ROAttr.new,
          'clusterRef' => ROAttr.new(ApiClusterRef),
          'configStale' => ROAttr.new,
          'configStalenessStatus' => ROAttr.new,
          'clientConfigStalenessStatus' => ROAttr.new,
          'serviceUrl' => ROAttr.new,
          'roleInstancesUrl' => ROAttr.new,
          'maintenanceMode' => ROAttr.new,
          'maintenanceOwners' => ROAttr.new,
          'entityStatus' => ROAttr.new
        }

        def initialize(resource_root, name = nil, type = nil)
          super(resource_root, { name: name, type: type })
        end

        def to_s
          "<ApiService>: #{@name} (cluster: #{_get_cluster_name})"
        end

        def _get_cluster_name
          if instance_variable_get('@clusterRef') && @clusterRef
            return @clusterRef.clusterName
          end
        end

        def _path
          # This method assumes that lack of a cluster reference means that the
          # object refers to the Cloudera Management Services instance.
          return format(SERVICE_PATH, _get_cluster_name, @name) if _get_cluster_name
          '/cm/service'
        end

        def get_commands(view = nil)
          _get('commands', ApiCommand, true, nil, view && { 'view' => view } || nil)
        end
      end
    end
  end
end
