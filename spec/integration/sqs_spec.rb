# encoding: utf-8
require "spec_helper"
require "logstash/inputs/sqs"
require "logstash/event"
require "logstash/json"
require "aws-sdk-sqs"
require_relative "../support/helpers"
require "thread"

Thread.abort_on_exception = true

describe "LogStash::Inputs::SQS integration", :integration => true do
  let(:decoded_message) { { "drstrange" => "is-he-really-that-strange" } }
  let(:encoded_message) { LogStash::Json.dump(decoded_message) }
  let(:queue) { Queue.new }

  let(:input) { LogStash::Inputs::SQS.new(options) }

  context "with invalid credentials" do
    let(:options) do
      {
        "queue" => "do-not-exist",
        "access_key_id" => "bad_access",
        "secret_access_key" => "bad_secret_key",
        "region" => ENV["AWS_REGION"]
      }
    end

    subject { input }

    it "raises a Configuration error if the credentials are bad" do
      expect { subject.register }.to raise_error(LogStash::ConfigurationError)
    end
  end

  context "with valid credentials" do
    let(:options) do
      {
        "queue" => ENV["SQS_QUEUE_NAME"],
        "access_key_id" => ENV['AWS_ACCESS_KEY_ID'],
        "secret_access_key" => ENV['AWS_SECRET_ACCESS_KEY'],
        "region" => ENV["AWS_REGION"]
      }
    end

    before :each do
      push_sqs_event(encoded_message)
      input.register
      @server = Thread.new { input.run(queue) }
    end

    after do
      @server.kill
    end

    subject { queue.pop }

    it "creates logstash events" do
      expect(subject["drstrange"]).to eq(decoded_message["drstrange"])
    end

    context "when the optionals fields are not specified" do
      let(:id_field) { "my_id_field" }
      let(:md5_field) { "my_md5_field" }
      let(:sent_timestamp_field) { "my_sent_timestamp_field" }

      it "add the `message_id`" do
        expect(subject[id_field]).to be_nil
      end

      it "add the `md5_of_body`" do
        expect(subject[md5_field]).to be_nil
      end

      it "add the `sent_timestamp`" do
        expect(subject[sent_timestamp_field]).to be_nil
      end

    end

    context "when the optionals fields are specified" do
      let(:id_field) { "my_id_field" }
      let(:md5_field) { "my_md5_field" }
      let(:sent_timestamp_field) { "my_sent_timestamp_field" }

      let(:options) do
        {
          "queue" => ENV["SQS_QUEUE_NAME"],
          "access_key_id" => ENV['AWS_ACCESS_KEY_ID'],
          "secret_access_key" => ENV['AWS_SECRET_ACCESS_KEY'],
          "region" => ENV["AWS_REGION"],
          "id_field" => id_field,
          "md5_field" => md5_field,
          "sent_timestamp_field" => sent_timestamp_field
        }
      end

      it "add the `message_id`" do
        expect(subject[id_field]).not_to be_nil
      end

      it "add the `md5_of_body`" do
        expect(subject[md5_field]).not_to be_nil
      end

      it "add the `sent_timestamp`" do
        expect(subject[sent_timestamp_field]).not_to be_nil
      end
    end
  end
end
