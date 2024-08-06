import Config

# Change aws_credentials so it does not affect testing
config :aws_credentials,
  credential_providers: [],
  fail_if_unavailable: false
