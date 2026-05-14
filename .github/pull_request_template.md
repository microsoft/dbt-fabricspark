> [!NOTE]
> Thank you for making change! Please fill this template for your pull request to improve quality of check-in message.

> [!TIP]
> This repo uses [Conventional Commit conventions](https://www.conventionalcommits.org/en/v1.0.0/) - please try to rename your PR headline to match it.

> [!WARNING]
> Please ensure to read through this whole set of instructions, specially the `Test` section.

# Why this change is needed

Describe what issue this change is trying to address.

If this is a bug fix, please describe

- How the bug was discovered.
- Is there a repro for the bug.

# How

Describe how the change works.

- What are some considerations that the reviewer should be aware of.
- Are there other known solutions and why this one is picked of them all?

# Test

## Important: Non-Microsoft Employee contributors

If you are not a Microsoft employee with a `foo@microsoft.com` email, you will not be able to run CI as it runs in the `@microsoft.com` Fabric Tenant where you do not have access.

In order for your PR to be considered for review, you **must** attach a clear screenshot of the output of you running the following command successfully:

```bash
npx nx run dbt-fabricspark:test --output-style=stream
```

Here's an example of a successful run:

![A successful CI run locally](https://rakirahman.blob.core.windows.net/public/images/Misc/dbt-fabricspark-ci-run-success.png)

> ⚠️ Delete the above image and attach your own screenshot

To keep the quality of the repo high, if you **do not** attach a screenshot of successful local testing, your PR will be promptly closed.

## Microsoft Employee contributors

Your PR will be subjected to full regression suite via GitHub Action.
It's highly recommended to run the tests locally so your contributions are promptly merged rather than failing in CI.