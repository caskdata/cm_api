#!/usr/bin/env ruby
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

require_relative 'types'
require_relative 'roles'

module CmApi
  module Endpoints
    # Module for role method and types
    module RoleConfigGroups
      include ::CmApi::Endpoints::Types

      ROLE_CONFIG_GROUPS_PATH = '/clusters/%s/services/%s/roleConfigGroups'.freeze
      CM_ROLE_CONFIG_GROUPS_PATH = '/cm/service/roleConfigGroups'.freeze

      def _get_role_config_groups_path(cluster_name, service_name)
        if !cluster_name.nil? && !cluster_name.empty?
          return format(ROLE_CONFIG_GROUPS_PATH, cluster_name, service_name)
        else
          return CM_ROLE_CONFIG_GROUPS_PATH
        end
      end

      def _get_role_config_group_path(cluster_name, service_name, name)
        path = _get_role_config_groups_path(cluster_name, service_name)
        format('%s/%s', path, name)
      end

      # Create role config groups.
      # @param service_name: Service name.
      # @param apigroup_list: List of role config groups to create.
      # @param cluster_name: Cluster name.
      # @return: New ApiRoleConfigGroup object.
      # @since: API v3
      def create_role_config_groups(service_name, apigroup_list, cluster_name = 'default')
        call_resource(@_resource_root.method(:post), _get_role_config_groups_path(cluster_name, service_name), ApiRoleConfigGroup, true, apigroup_list, nil, 3)
      end

      # Create a role config group.
      # @param service_name: Service name.
      # @param name: The name of the new group.
      # @param display_name: The display name of the new group.
      # @param role_type: The role type of the new group.
      # @param cluster_name: Cluster name.
      # @return: List of created role config groups.
      def create_role_config_group(service_name, name, display_name, role_type, cluster_name = 'default')
        apigroup = ApiRoleConfigGroup.new(self, name, display_name, role_type)
        create_role_config_groups(service_name, [apigroup], cluster_name)[0]
      end

      # Find a role config group by name.
      # @param service_name: Service name.
      # @param name: Role config group name.
      # @param cluster_name: Cluster name.
      # @return: An ApiRoleConfigGroup object.
      def get_role_config_group(service_name, name, cluster_name = 'default')
        _get_role_config_group(_get_role_config_group_path(cluster_name, service_name, name))
      end

      def _get_role_config_group(path)
        call_resource(@_resource_root.method(:get), path, ApiRoleConfigGroup, false, nil, nil, 3)
      end

      # Get all role config groups in the specified service.
      # @param service_name: Service name.
      # @param cluster_name: Cluster name.
      # @return: A list of ApiRoleConfigGroup objects.
      # @since: API v3
      def get_all_role_config_groups(service_name, cluster_name = 'default')
        call_resource(@_resource_root.method(:get), _get_role_config_groups_path(cluster_name, service_name), ApiRoleConfigGroup, true, nil, nil, 3)
      end

      # Update a role config group by name.
      # @param service_name: Service name.
      # @param name: Role config group name.
      # @param apigroup: The updated role config group.
      # @param cluster_name: Cluster name.
      # @return: The updated ApiRoleConfigGroup object.
      # @since: API v3
      def update_role_config_group(service_name, name, apigroup, cluster_name = 'default')
        call_resource(@_resource_root.method(:put), _get_role_config_group_path(cluster_name, service_name, name), ApiRoleConfigGroup, false, apigroup, nil, 3)
      end 

      # Delete a role config group by name.
      # @param service_name: Service name.
      # @param name: Role config group name.
      # @param cluster_name: Cluster name.
      # @return: The deleted ApiRoleConfigGroup object.
      # @since: API v3
      def delete_role_config_group(service_name, name, cluster_name = 'default')
        call_resource(@_resource_root.method(:delete), _get_role_config_group_path(cluster_name, service_name, name), ApiRoleConfigGroup, false, nil, nil, 3)
      end

      # Moves roles to the specified role config group.
      #
      # The roles can be moved from any role config group belonging
      # to the same service. The role type of the destination group
      # must match the role type of the roles.
      #
      # @param name: The name of the group the roles will be moved to.
      # @param role_names: The names of the roles to move.
      # @return: List of roles which have been moved successfully.
      # @since: API v3
      def move_roles(service_name, name, role_names, cluster_name = 'default')
        call_resource(@_resource_root.method(:put), _get_role_config_group_path(cluster_name, service_name, name) + '/roles', ApiRole, true, role_names, nil, 3)
      end

      # Moves roles to the base role config group.
      #
      # The roles can be moved from any role config group belonging to the same
      # service. The role type of the roles may vary. Each role will be moved to
      # its corresponding base group depending on its role type.
      #
      # @param role_names: The names of the roles to move.
      # @return: List of roles which have been moved successfully.
      # @since: API v3
      def move_roles_to_base_role_config_group(service_name, role_names, cluster_name = 'default')
        call_resource(@_resource_root.method(:put), _get_role_config_groups_path(cluster_name, service_name) + '/roles', ApiRole, true, role_names, nil, 3)
      end

      # Model for a RoleConfigGroup
      class ApiRoleConfigGroup < BaseApiResource
        # name is RW only temporarily; once all RCG names are unique,
        # this property will be auto-generated and Read-only
        @_ATTRIBUTES = {
          'name' => nil,
          'displayName' => nil,
          'roleType' => nil,
          'config' => Attr.new(ApiConfig),
          'base' => ROAttr.new,
          'serviceRef' => ROAttr.new(ApiServiceRef)
        }

        def initialize(resource_root, name = nil, displayName = nil, roleType = nil, config = nil)
          super(resource_root, {name: name, displayName: displayName, roleType: roleType, config: config})
        end

        def to_s
          "<ApiRoleConfigGroup>: #{@name} (cluster: #{@serviceRef.clusterName}; service: #{@serviceRef.serviceName})"
        end

        def _api_version
          3
        end

        def _path
          _get_role_config_group_path(@serviceRef.clusterName, @serviceRef.serviceName, @name)
        end

        # Retrieve the group's configuration.
        #
        # The 'summary' view contains strings as the dictionary values. The full
        # view contains ApiConfig instances as the values.
        #
        # @param view: View to materialize ('full' or 'summary').
        # @return: Dictionary with configuration data.
        def get_config(view = nil)
          path = _path() + '/config'
          resp = @_resource_root.get(path, view && { 'view' => view } || nil)
          json_to_config(resp, view == 'full')
        end

        # Update the group's configuration.
        #
        # @param config: Dictionary with configuration to update.
        # @return: Dictionary with updated configuration.
        def update_config(config)
          path = _path() + '/config'
          resp = @_resource_root.put(path, nil, config_to_json(config))
          json_to_config(resp)
        end

        # Retrieve the roles in this role config group.
        #
        # @return: List of roles in this role config group.
        def get_all_roles
          _get('roles', ApiRole, true)
        end

        # Moves roles to this role config group.
        #
        # The roles can be moved from any role config group belonging
        # to the same service. The role type of the destination group
        # must match the role type of the roles.
        #
        # @param roles: The names of the roles to move.
        # @return: List of roles which have been moved successfully.
        def move_roles(roles)
          # Ruby Port note: this calls a method of the same name in the module... not sure this will work
          move_roles(@serviceRef.serviceName, @name, roles, @serviceRef.clusterName)
        end
      end
    end
  end
end
