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

require 'date'

require_relative 'types'

module CmApi
  module Endpoints
    # Module for parcel methods and types
    module Parcels
      include ::CmApi::Endpoints::Types

      PARCELS_PATH = '/clusters/%s/parcels'.freeze
      PARCEL_PATH = '/clusters/%s/parcels/products/%s/versions/%s'.freeze

      # Lookup a parcel by name
      # @param product: Parcel product name
      # @param version: Parcel version
      # @param cluster_name: Cluster name
      # @return: An ApiParcel object
      def get_parcel(product, version, cluster_name = 'default')
        return _get_parcel( format(PARCEL_PATH, cluster_name, product, version))
      end

      def _get_parcel(path)
        call_resource(method(:get), path, ApiParcel, false, nil, nil, 3)
      end

      # Get all parcels
      # @param resource_root: The root Resource object.
      # @param cluster_name: Cluster name
      # @return: A list of ApiParcel objects.
      # @since: API v3
      def get_all_parcels(cluster_name = 'default', view = nil)
        call_resource(method(:get), format(PARCELS_PATH, cluster_name), ApiParcel, true, nil, view && { 'view' => view } || nil, 3)
      end

      # An object that represents the state of a parcel.
      class ApiParcelState < BaseApiObject
        @_ATTRIBUTES = {
          'progress' => ROAttr.new,
          'totalProgress' => ROAttr.new,
          'count' => ROAttr.new,
          'totalCount' => ROAttr.new,
          'warnings' => ROAttr.new,
          'errors' => ROAttr.new
        }

        def initialize(resource_root)
          super(resource_root)
        end

        def to_s
          "<ApiParcelState>: (progress: #{@progress}) (totalProgress: #{@totalProgress}) (count: #{@count}) (totalCount: #{@totalCount})"
        end
      end

      # An object that represents a parcel and allows administrative operations.
      class ApiParcel < BaseApiResource
        @_ATTRIBUTES = {
          'product' => ROAttr.new,
          'version' => ROAttr.new,
          'stage' => ROAttr.new,
          'state' => ROAttr.new(ApiParcelState),
          'clusterRef' => ROAttr.new(ApiClusterRef)
        }

        def initialize(resource_root)
          super(resource_root)
        end
  
        def to_s
          "<ApiParcel>: #{@product}-#{@version} (stage: #{@stage}) (state: #{@state}) (cluster: #{_get_cluster_name})"
        end
  
        def _api_version
          return 3
        end
  
        # Return the API path for this service.
        def _path
          return format(PARCEL_PATH, _get_cluster_name, @product, @version)
        end
  
        def _get_cluster_name
          if instance_variable_get('@clusterRef') && @clusterRef
            return @clusterRef.clusterName
          end
        end
  
        # Start the download of the parcel
        #
        # @return: Reference to the completed command.
        def start_download
          _cmd('startDownload')
        end
  
        # Cancels the parcel download. If the parcel is not
        # currently downloading an exception is raised.
        # 
        # @return: Reference to the completed command.
        def cancel_download
          _cmd('cancelDownload')
        end
  
        # Removes the downloaded parcel
        #
        # @return: Reference to the completed command.
        def remove_download
          _cmd('removeDownload')
        end
  
        # Start the distribution of the parcel to all hosts
        # in the cluster.
        #
        # @return: Reference to the completed command.
        def start_distribution
          _cmd('startDistribution')
        end
  
        # Cancels the parcel distrubution. If the parcel is not
        # currently distributing an exception is raised.
        #
        # @return: Reference to the completed command
        def cancel_distribution
          _cmd('cancelDistribution')
        end
  
        # Start the removal of the distribution of the parcel
        # from all the hosts in the cluster.
        #
        # @return: Reference to the completed command.
        def start_removal_of_distribution
          _cmd('startRemovalOfDistribution')
        end
  
        # Activate the parcel on all the hosts in the cluster.
        #
        # @return: Reference to the completed command.
        def activate
          _cmd('activate')
        end
  
        # Deactivates the parcel on all the hosts in the cluster.
        #
        # @return: Reference to the completed command.
        def deactivate
          _cmd('deactivate')
        end
      end
    end
  end
end
