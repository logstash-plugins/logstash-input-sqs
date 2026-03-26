# encoding: utf-8
#
require "logstash/inputs/threadable"
require "logstash/namespace"
require "logstash/timestamp"
require "logstash/plugin_mixins/aws_config"
require "logstash/errors"

# Pull events from an Amazon Web Services Simple Queue Service (SQS) queue.
#
# SQS is a simple, scalable queue system that is part of the
# Amazon Web Services suite of tools.
#
# Although SQS is similar to other queuing systems like AMQP, it
# uses a custom API and requires that you have an AWS account.
# See http://aws.amazon.com/sqs/ for more details on how SQS works,
# what the pricing schedule looks like and how to setup a queue.
#
# To use this plugin, you *must*:
#
#  * Have an AWS account
#  * Setup an SQS queue
#  * Create an identify that has access to consume messages from the queue.
#
# The "consumer" identity must have the following permissions on the queue:
#
#  * `sqs:ChangeMessageVisibility`
#  * `sqs:ChangeMessageVisibilityBatch`
#  * `sqs:DeleteMessage`
#  * `sqs:DeleteMessageBatch`
#  * `sqs:GetQueueAttributes`
#  * `sqs:GetQueueUrl`
#  * `sqs:ListQueues`
#  * `sqs:ReceiveMessage`
#
# Typically, you should setup an IAM policy, create a user and apply the IAM policy to the user.
# A sample policy is as follows:
# [source,json]
#     {
#       "Statement": [
#         {
#           "Action": [
#             "sqs:ChangeMessageVisibility",
#             "sqs:ChangeMessageVisibilityBatch",
#             "sqs:GetQueueAttributes",
#             "sqs:GetQueueUrl",
#             "sqs:ListQueues",
#             "sqs:SendMessage",
#             "sqs:SendMessageBatch"
#           ],
#           "Effect": "Allow",
#           "Resource": [
#             "arn:aws:sqs:us-east-1:123456789012:Logstash"
#           ]
#         }
#       ]
#     }
#
# See http://aws.amazon.com/iam/ for more details on setting up AWS identities.
#
class LogStash::Inputs::SQS < LogStash::Inputs::Threadable
  include LogStash::PluginMixins::AwsConfig::V2

  MAX_TIME_BEFORE_GIVING_UP = 60
  MAX_MESSAGES_TO_FETCH = 10 # Between 1-10 in the AWS-SDK doc
  SENT_TIMESTAMP = "SentTimestamp"
  SQS_ATTRIBUTES = [SENT_TIMESTAMP]
  BACKOFF_SLEEP_TIME = 1
  BACKOFF_FACTOR = 2
  DEFAULT_POLLING_FREQUENCY = 20

  config_name "sqs"

  default :codec, "json"

  config :additional_settings, :validate => :hash, :default => {}

  # Name of the SQS Queue name to pull messages from. Note that this is just the name of the queue, not the URL or ARN.
  config :queue, :validate => :string, :required => true

  # Account ID of the AWS account which owns the queue.
  config :queue_owner_aws_account_id, :validate => :string, :required => false

  # Name of the event field in which to store the SQS message ID
  config :id_field, :validate => :string

  # Name of the event field in which to store the SQS message MD5 checksum
  config :md5_field, :validate => :string

  # Name of the event field in which to store the SQS message Sent Timestamp
  config :sent_timestamp_field, :validate => :string

  # Polling frequency, default is 20 seconds
  config :polling_frequency, :validate => :number, :default => DEFAULT_POLLING_FREQUENCY

  attr_reader :poller

  def register
    require "aws-sdk-sqs"
    @logger.info("Registering SQS input", :queue => @queue, :queue_owner_aws_account_id => @queue_owner_aws_account_id)

    setup_queue
  end

  def queue_url(aws_sqs_client)
    if @queue_owner_aws_account_id
      return aws_sqs_client.get_queue_url({:queue_name => @queue, :queue_owner_aws_account_id => @queue_owner_aws_account_id})[:queue_url]
    else
      return aws_sqs_client.get_queue_url(:queue_name => @queue)[:queue_url]
    end
  end

  def setup_queue
    aws_sqs_client = Aws::SQS::Client.new(aws_options_hash || {})
    poller = Aws::SQS::QueuePoller.new(queue_url(aws_sqs_client), :client => aws_sqs_client)
    poller.before_request { |stats| throw :stop_polling if stop? }

    @poller = poller
  rescue Aws::SQS::Errors::ServiceError, Seahorse::Client::NetworkingError => e
    @logger.error("Cannot establish connection to Amazon SQS", exception_details(e))
    raise LogStash::ConfigurationError, "Verify the SQS queue name and your credentials"
  end

  def polling_options
    { 
      :max_number_of_messages => MAX_MESSAGES_TO_FETCH,
      :attribute_names => SQS_ATTRIBUTES,
      :wait_time_seconds => @polling_frequency
    }
  end

  def add_sqs_data(event, message)
    event.set(@id_field, message.message_id) if @id_field
    event.set(@md5_field, message.md5_of_body) if @md5_field
    event.set(@sent_timestamp_field, convert_epoch_to_timestamp(message.attributes[SENT_TIMESTAMP])) if @sent_timestamp_field
    event
  end

  def handle_message(message, output_queue)
    @codec.decode(message.body) do |event|
      add_sqs_data(event, message)
      decorate(event)
      output_queue << event
    end
  end

  def run(output_queue)
    @logger.debug("Polling SQS queue", :polling_options => polling_options)

    run_with_backoff do
      poller.poll(polling_options) do |messages, stats|
        break if stop?
        messages.each {|message| handle_message(message, output_queue) }
        @logger.debug("SQS Stats:", :request_count => stats.request_count,
                      :received_message_count => stats.received_message_count,
                      :last_message_received_at => stats.last_message_received_at) if @logger.debug?
      end
    end
  end

  private

  # Runs an AWS request inside a Ruby block with an exponential backoff in case
  # we experience a ServiceError.
  #
  # @param [Block] block Ruby code block to execute.
  def run_with_backoff(&block)
    sleep_time = BACKOFF_SLEEP_TIME
    begin
      block.call
    rescue Aws::SQS::Errors::ServiceError, Seahorse::Client::NetworkingError => e
      @logger.warn("SQS error ... retrying with exponential backoff", exception_details(e, sleep_time))
      sleep_time = backoff_sleep(sleep_time)
      retry
    end
  end

  def backoff_sleep(sleep_time)
    sleep(sleep_time)
    sleep_time > MAX_TIME_BEFORE_GIVING_UP ? sleep_time : sleep_time * BACKOFF_FACTOR
  end

  def convert_epoch_to_timestamp(time)
    LogStash::Timestamp.at(time.to_i / 1000)
  end

  def exception_details(e, sleep_time = nil)
    details = { :queue => @queue, :exception => e.class, :message => e.message }
    details[:code] = e.code if e.is_a?(Aws::SQS::Errors::ServiceError) && e.code
    details[:cause] = e.original_error if e.respond_to?(:original_error) && e.original_error # Seahorse::Client::NetworkingError
    details[:sleep_time] = sleep_time if sleep_time
    details[:backtrace] = e.backtrace if @logger.debug?
    details
  end

end # class LogStash::Inputs::SQS
