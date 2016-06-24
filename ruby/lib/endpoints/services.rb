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

#require 'date'

require_relative 'types'

module CmApi
  module Endpoints
    module Services 

      include ::CmApi::Endpoints::Types
      #include ::CmApi::Endpoints::Roles

      SERVICES_PATH = '/clusters/%s/services'
      SERVICE_PATH = '/clusters/%s/services/%s'
      ROLETYPES_CFG_KEY = 'roleTypeConfigs'

      def create_service(resource_root, name, service_type, cluster_name = 'default')
        apiservice = ApiService.new(resource_root, name, service_type)
        return call(resource_root.method(:post), SERVICES_PATH % [cluster_name], ApiService, true, [apiservice])[0]
      end

      def get_service(resource_root, name, cluster_name = 'default')
        return _get_service(resource_root, "%s/%s" % [ (SERVICES_PATH % cluster_name), name ] )
      end

      def _get_service(resource_root, path)
        return call(resource_root.method(:get), path, ApiService)
      end

      def get_all_services(resource_root, cluster_name = 'default', view = nil)
        return call(resource_root.method(:get), SERVICES_PATH % [cluster_name], ApiService, true, nil, view && { 'view' => view } || nil)
      end

      def delete_service(resource_root, name, cluster_name = 'default')
        return call(resource_root.method(:delete), "%s/%s" % [ (SERVICES_PATH % cluster_name), name ], ApiService)
      end

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
          super(resource_root, {:name => name, :type => type })
        end

        def to_s()
          return "<ApiService>: #{@name} (cluster: #{_get_cluster_name})"
        end

        def _get_cluster_name()
          if instance_variable_get('@clusterRef') && @clusterRef
            return @clusterRef.clusterName
          end
        end

        def _path()
          # This method assumes that lack of a cluster reference means that the
          # object refers to the Cloudera Management Services instance.
          if _get_cluster_name()
            return SERVICE_PATH % [_get_cluster_name, @name]
          end 
          return '/cm/service'
        end

        def get_commands(view = nil)
          return _get('commands', ApiCommand, true, nil, view && { 'view' => view } || nil)
        end

      end 
    end
  end
end
