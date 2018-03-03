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

require 'rest-client'
require 'uri'

module CmApi
  # Any error result from the Rest API is converted into this exception type.
  class RestException < RuntimeError
    attr_reader :code, :message

    def initialize(error)
      @error = error
      @code = nil
      @message = error.to_s

      if error.respond_to?(:response)
        @code = error.response.code if error.response.respond_to?(:code)
      end
    end
  end

  # TODO: add logger
  # TODO: use Net::HTTP instead of rest-client
  # Basic HTTP client tailored for rest APIs.
  class HttpClient
    attr_reader :base_url
    attr_accessor :headers

    # Creates an HTTP(S) client to connect to the Cloudera Manager API.
    #   @param base_url: The base url to the API.
    #   @param exc_class: An exception class to handle non-200 results.
    def initialize(base_url, exc_class = nil)
      @base_url = base_url.chomp('/')
      @exc_class = exc_class || RestException
      @headers = {}
    end

    def _get_headers(headers)
      # return static headers plus additional
      if headers
        @headers.merge(headers)
      else
        @headers
      end
    end

    def set_basic_auth(username, password, _realm = nil)
      @user = username
      @password = password
    end

    # Submit an HTTP request.
    #   @param http_method: GET, POST, PUT, DELETE
    #   @param path: The path of the resource.
    #   @param params: Key-value parameter data.
    #   @param data: The data to attach to the body of the request.
    #   @param headers: The headers to set for this request.
    #   @return: HTTP response
    def execute(http_method, path, params = nil, data = nil, headers = nil)
      # Prepare URL and params
      url = _make_url(path, params)
      if [:get, :delete].include? http_method
        unless data.nil?
          puts "WARNING: #{http_method.to_s.upcase} method does not pass any data. Path '#{path}'"
          data = nil
        end
      end

      # Setup the request
      rest_client_args = {}
      rest_client_args[:method] = http_method
      rest_client_args[:url] = url
      rest_client_args[:payload] = data
      rest_client_args[:user] = @user
      rest_client_args[:password] = @password
      rest_client_args[:headers] = _get_headers(headers)

      # Execute
      # TODO: add logger
      begin
        ::RestClient::Request.execute(rest_client_args)
      rescue => e
        raise @exc_class.new(e)
      end
    end

    def _make_url(path, params)
      res = @base_url
      res = ::File.join(res, path.chomp('/')).to_s if path
      if params
        param_str = ::URI.encode_www_form(params)
        res += '?' + param_str
      end
      res
    end
  end
end
