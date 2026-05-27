#!/usr/bin/env bash
# Tear down the CFTL-656 migration-test configs + their derived Storage buckets on cf-dev.
# Safe to re-run: missing configs/buckets are skipped. Requires kbagent with a cf-dev project.
set -u

PROJECT="cf-dev"

# slug : component-id : cohort : config-id  (created by the migration test run)
CONFIGS=(
  "instagram:keboola.ex-instagram-v2:A:01ksmrq4tc62rjdkqasxsnxz0m"
  "instagram:keboola.ex-instagram-v2:B:01ksmrq6a27jdknx71mbcstx9d"
  "instagram:keboola.ex-instagram-v2:C:01ksmrq7ntrejf5w3hbgbx2ng8"
  "fbpages:keboola.ex-facebook-pages:A:01ksmrq9c7snxeb5qphdy303sv"
  "fbpages:keboola.ex-facebook-pages:B:01ksmrqapgwcq6zggwbye05jdp"
  "fbpages:keboola.ex-facebook-pages:C:01ksmrqc27zspanyd0480cmm47"
  "fbads:keboola.ex-facebook-ads-v2:A:01ksmrqdgz1yrtv9nww0yrrhzq"
  "fbads:keboola.ex-facebook-ads-v2:B:01ksmrqey5hehvd2q51rwkcbfy"
  "fbads:keboola.ex-facebook-ads-v2:C:01ksmrqgg6j6m05na70yrgymqx"
)

bucket_prefix() {
  # component-id dots -> dashes
  echo "in.c-${1//./-}-${2}"
}

for spec in "${CONFIGS[@]}"; do
  IFS=":" read -r slug comp cohort cid <<<"$spec"
  bucket="$(bucket_prefix "$comp" "$cid")"
  echo "=== ${slug} cohort ${cohort} (${cid}) ==="
  echo "  deleting bucket ${bucket} (with tables)"
  kbagent --json storage delete-bucket --project "$PROJECT" --bucket-id "$bucket" --force >/dev/null 2>&1 \
    && echo "    bucket removed" || echo "    bucket absent / already removed"
  echo "  deleting config ${cid}"
  kbagent --json config delete --project "$PROJECT" --component-id "$comp" --config-id "$cid" >/dev/null 2>&1 \
    && echo "    config removed" || echo "    config absent / already removed"
done

echo "cleanup complete"
