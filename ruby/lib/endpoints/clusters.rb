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

require_relative 'types'

module CmApi
  module Endpoints
    # Module for Cluster methods and types
    module Clusters
      include ::CmApi::Endpoints::Types

      CLUSTERS_PATH = '/clusters'.freeze

      def create_cluster(name, version = nil, fullVersion = nil)
        if version.nil? && fullVersion.nil?
          raise "Either 'version' or 'fullVersion' must be specified"
        end
        if !fullVersion.nil?
          api_version = 6
          version = nil
        else
          api_version = 1
        end

        apicluster = ApiCluster.new(self, name, version, fullVersion)
        # Ruby port note: as this module is included directly into a resource (ApiResource), we call
        # the :get/:post method directly.  In other BaseApiObject-derived modules (Services/Roles), we must call the
        # method on the @_resource_root instance variable object (ApiResource)
        call_resource(method(:post), CLUSTERS_PATH, ApiCluster, true, [apicluster], nil, api_version)[0]
      end

      def get_cluster(name)
        call_resource(method(:get), "#{CLUSTERS_PATH}/#{name}", ApiCluster)
      end

      def get_all_clusters(view = nil)
        call_resource(method(:get), CLUSTERS_PATH, ApiCluster, true, nil, view && { 'view' => view } || nil)
      end

      def delete_cluster(name)
        call_resource(method(:delete), "#{CLUSTERS_PATH}/#{name}", ApiCluster)
      end

      # Model for a cluster
      class ApiCluster < BaseApiResource
        @_ATTRIBUTES = {
          'name' => nil,
          'displayName' => nil,
          'clusterUrl' => nil,
          'version' => nil,
          'fullVersion' => nil,
          'hostsUrl' => ROAttr.new,
          'maintenanceMode' => ROAttr.new,
          'maintenanceOwners' => ROAttr.new,
          'entityStatus' => ROAttr.new
        }

        def initialize(resource_root, name = nil, version = nil, fullVersion = nil)
          # possible alternative to generate the hash argument dynamically, similar to python locals():
          #  method(__method__).parameters.map { |arg| arg[1] }.inject({}) { |h, a| h[a] = eval a.to_s; h}
          super(resource_root, { name: name, version: version, fullVersion: fullVersion })
        end

        def to_s
          "<ApiCluster>: #{@name}; version: #{@version}"
        end

        def _path
          "#{CLUSTERS_PATH}/#{@name}"
        end

        def _put_cluster(dic, params = nil)
          cluster = _put('', ApiCluster, false, dic, params)
          _update(cluster)
        end

        def get_service_types
          resp = @_resource_root.get(_path + '/serviceTypes')
          resp[ApiList::LIST_KEY]
        end

        def get_commands(view = nil)
          _get('commands', ApiCommand, true, view && { 'view' => view } || nil)
        end

        def list_hosts
          _get('hosts', ApiHostRef, true, nil, 3)
        end
      end
    end
  end
end
