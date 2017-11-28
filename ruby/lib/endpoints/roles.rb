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
    # Module for role method and types
    module Roles
      include ::CmApi::Endpoints::Types

      ROLES_PATH = '/clusters/%s/services/%s/roles'.freeze
      CM_ROLES_PATH = '/cm/service/roles'.freeze

      def _get_roles_path(cluster_name, service_name)
        if !cluster_name.nil? && !cluster_name.empty?
          return format(ROLES_PATH, cluster_name, service_name)
        else
          return CM_ROLES_PATH
        end
      end

      def _get_role_path(cluster_name, service_name, role_name)
        path = _get_roles_path(cluster_name, service_name)
        format('%s/%s', path, role_name)
      end

      # Create a role
      # @param service_name: Service name
      # @param role_type: Role type
      # @param role_name: Role name
      # @param cluster_name: Cluster name
      # @return: An ApiRole object
      def create_role(service_name, role_type, role_name, host_id, cluster_name = 'default')
        apirole = ApiRole.new(self, role_name, role_type, ApiHostRef.new(self, host_id))
        call_resource(@_resource_root.method(:post), _get_roles_path(cluster_name, service_name), ApiRole, true, [apirole])[0]
      end

      # Lookup a role by name
      # @param service_name: Service name
      # @param name: Role name
      # @param cluster_name: Cluster name
      # @return: An ApiRole object
      def get_role(service_name, name, cluster_name = 'default')
        _get_role(_get_role_path(cluster_name, service_name, name))
      end

      def _get_role(path)
        call_resource(@_resource_root.method(:get), path, ApiRole)
      end

      # Get all roles
      # @param service_name: Service name
      # @param cluster_name: Cluster name
      # @return: A list of ApiRole objects.
      def get_all_roles(service_name, cluster_name = 'default', view = nil)
        call_resource(@_resource_root.method(:get), _get_roles_path(cluster_name, service_name), ApiRole, true, nil, view && { 'view' => view } || nil)
      end

      # Get all roles of a certain type in a service
      # @param service_name: Service name
      # @param role_type: Role type
      # @param cluster_name: Cluster name
      # @return: A list of ApiRole objects.
      def get_roles_by_type(service_name, role_type, cluster_name = 'default', view = nil)
        roles = get_all_roles(service_name, cluster_name, view)
        roles.select { |r| r.type == role_type }
      end

      def delete_role(service_name, name, cluster_name = 'default')
        call_resource(@_resource_root.method(:delete), _get_role_path(cluster_name, service_name, name), ApiRole)
      end

      # Model for a role
      class ApiRole < BaseApiResource
        @_ATTRIBUTES = {
          'name' => nil,
          'type' => nil,
          'hostRef' => Attr.new(ApiHostRef),
          'roleState' => ROAttr.new,
          'healthSummary' => ROAttr.new,
          'healthChecks' => ROAttr.new,
          'serviceRef' => ROAttr.new(ApiServiceRef),
          'configStale' => ROAttr.new,
          'configStalenessStatus' => ROAttr.new,
          'haStatus' => ROAttr.new,
          'roleUrl' => ROAttr.new,
          'commissionState' => ROAttr.new,
          'maintenanceMode' => ROAttr.new,
          'maintenanceOwners' => ROAttr.new,
          'roleConfigGroupRef' => ROAttr.new(ApiRoleConfigGroupRef),
          'zookeeperServerMode' => ROAttr.new,
          'entityStatus' => ROAttr.new
        }

        def initialize(resource_root, name = nil, type = nil, hostRef = nil)
          super(resource_root, { name: name, type: type, hostRef: hostRef })
        end

        def to_s
          "<ApiRole>: #{@name} (cluster: #{@serviceRef.clusterName}; service: #{@serviceRef.serviceName})"
        end

        def _path
          _get_role_path(@serviceRef.clusterName, @serviceRef.serviceName, @name)
        end

        def _get_log(log)
          path = format('%s/logs/%s', _path, log)
          @_resource_root._get(path)
        end

        # Retrieve a list of running commands for this role.
        # @param view: View to materialize ('full' or 'summary')
        # @return: A list of running commands.
        def get_commands(view = nil)
          _get('commands', ApiCommand, true, nil, view && { 'view' => view } || nil)
        end

        # Retrieve the role's configuration.
        # The 'summary' view contains strings as the dictionary values. The full
        # view contains ApiConfig instances as the values.
        # @param view: View to materialize ('full' or 'summary')
        # @return: Dictionary with configuration data.
        def get_config(view = nil)
          _get_config('config', view)
        end

        # Update the role's configuration.
        # @param config: Dictionary with configuration to update.
        # @return: Dictionary with updated configuration.
        def update_config(config)
          _update_config('config', config)
        end

        # Retrieve the contents of the role's log file.
        # @return: Contents of log file.
        def get_full_log
          _get_log('full')
        end

        # Retrieve the contents of the role's standard output.
        # @return: Contents of stdout.
        def get_stdout
          _get_log('stdout')
        end

        # Retrieve the contents of the role's standard error.
        # @return: Contents of stderr.
        def get_stderr
          _get_log('stderr')
        end

        # Retrieve the contents of the role's stacks log file.
        # @return: Contents of stacks log file.
        # @since: API v8
        def get_stacks_log
          _get_log('stacks')
        end

        # Retrieve a zip file of the role's stacks log files.
        # @return: A zipfile of stacks log files.
        # @since: API v8
        def get_stacks_log_bundle
          _get_log('stacksBundle')
        end

        # Put the role in maintenance mode.
        # @return: Reference to the completed command.
        # @since: API v2
        def enter_maintenance_mode
          cmd = _cmd('enterMaintenanceMode')
          _update(_get_role(_path)) if cmd.success
          cmd
        end

        # Take the role out of maintenance mode.
        # @return: Reference to the completed command.
        # @since: API v2
        def exit_maintenance_mode
          cmd = _cmd('exitMaintenanceMode')
          _update(_get_role(_path)) if cmd.success
          cmd
        end

        # Lists all the commands that can be executed by name
        # on the provided role.
        # @return: A list of command metadata objects
        # @since: API v6
        def list_commands_by_name
          _get('commandsByName', ApiCommandMetadata, true, nil, nil, 6)
        end
      end
    end
  end
end
