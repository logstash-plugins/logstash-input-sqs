# 1.1.0
- AWS ruby SDK v2 upgrade
- Replaces aws-sdk dependencies with mixin-aws
- Removes unnecessary de-allocation
- Move the code into smaller methods to allow easier mocking and testing
- Add the option to configure polling frequency
- Adding a monkey patch to make sure `LogStash::ShutdownSignal` doesn't get catch by AWS RetryError.
