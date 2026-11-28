//! The lake's objects over S3 (decisions 0035, 0055 and 0078): a cut's
//! files written once, with credentials issued for that write by STS and
//! limited by the caller's session policy, and `If-None-Match: *`, so the
//! lake itself refuses to overwrite one (`tools/verify/lake_manifests.py`);
//! and a table dropped with its files, its files first while the catalog
//! still names it where the lake declares a drop does not purge them
//! (`tools/verify/lake_drop.py`), else by the catalog with
//! `purgeRequested`. Objects are addressed as the lake declares
//! (`UrlStyle`).

use std::time::Duration;

use application::lake::{LakeProperties, UrlStyle};
use chrono::Utc;

use crate::lake_catalog::TableLookup;
use crate::lake_sts::StsIssuer;
use sigv4::{encode, payload_hash};

/// The URL of `key` in `bucket` (an empty key: the bucket), as the lake
/// addresses objects.
pub(crate) fn object_url(properties: &LakeProperties, bucket: &str, key: &str) -> String {
    let base = properties.s3_endpoint.trim_end_matches('/');
    let key = encode(key, true);
    match properties.url_style {
        UrlStyle::Path => format!("{base}/{}/{key}", encode(bucket, false)),
        UrlStyle::VirtualHosted => match base.split_once("://") {
            Some((scheme, host)) => format!("{scheme}://{bucket}.{host}/{key}"),
            None => format!("{bucket}.{base}/{key}"),
        },
    }
}

/// Writes manifests through the platform's STS issuer.
#[derive(Debug, Clone)]
pub struct ManifestWriter {
    issuer: StsIssuer,
}

impl ManifestWriter {
    pub fn new(issuer: StsIssuer) -> Self {
        Self { issuer }
    }

    /// Writes `body` at `bucket/key` unless an object is there; `Ok(false)`
    /// if one was. `policy` is the session policy for this write.
    pub async fn put_once(
        &self,
        bucket: &str,
        key: &str,
        body: Vec<u8>,
        policy: &str,
    ) -> Result<bool, String> {
        let creds = self
            .issuer
            .assume_role("manifest", policy, Duration::from_secs(0))
            .await
            .map_err(|e| e.to_string())?;
        let url = object_url(self.issuer.properties(), bucket, key);
        let path = format!(
            "/{}",
            url.split_once("://")
                .and_then(|(_, r)| r.split_once('/'))
                .map_or("", |(_, p)| p)
        );
        let host = sigv4::host(&url).ok_or_else(|| format!("bad endpoint {url}"))?;
        let hash = payload_hash(&body);
        let headers = [
            ("content-type", "application/json"),
            ("host", host.as_str()),
            ("if-none-match", "*"),
            ("x-amz-content-sha256", hash.as_str()),
            ("x-amz-security-token", creds.session_token.as_str()),
        ];
        let signed = sigv4::sign(
            "s3",
            &creds.access_key,
            &creds.secret_key,
            "PUT",
            &path,
            &headers,
            Utc::now(),
        );
        let mut req = self.issuer.http().put(&url).body(body);
        for (name, value) in headers.iter().filter(|(n, _)| *n != "host") {
            req = req.header(*name, *value);
        }
        let res = req
            .header("x-amz-date", &signed.date)
            .header("authorization", &signed.authorization)
            .send()
            .await
            .map_err(|e| format!("manifest write: {e}"))?;
        match res.status().as_u16() {
            200 => Ok(true),
            412 => Ok(false),
            status => Err(format!("manifest write refused ({status})")),
        }
    }
}

impl ManifestWriter {
    /// The object at `bucket/key`, or `None` if there is none. `policy` is
    /// the session policy of this read.
    pub async fn get(
        &self,
        bucket: &str,
        key: &str,
        policy: &str,
    ) -> Result<Option<Vec<u8>>, String> {
        let creds = self
            .issuer
            .assume_role("manifest-read", policy, Duration::from_secs(0))
            .await
            .map_err(|e| e.to_string())?;
        let url = object_url(self.issuer.properties(), bucket, key);
        let path = format!(
            "/{}",
            url.split_once("://")
                .and_then(|(_, r)| r.split_once('/'))
                .map_or("", |(_, p)| p)
        );
        let host = sigv4::host(&url).ok_or_else(|| format!("bad endpoint {url}"))?;
        let hash = payload_hash(&[]);
        let headers = [
            ("host", host.as_str()),
            ("x-amz-content-sha256", hash.as_str()),
            ("x-amz-security-token", creds.session_token.as_str()),
        ];
        let signed = sigv4::sign(
            "s3",
            &creds.access_key,
            &creds.secret_key,
            "GET",
            &path,
            &headers,
            Utc::now(),
        );
        let mut req = self.issuer.http().get(&url);
        for (name, value) in headers.iter().filter(|(n, _)| *n != "host") {
            req = req.header(*name, *value);
        }
        let res = req
            .header("x-amz-date", &signed.date)
            .header("authorization", &signed.authorization)
            .send()
            .await
            .map_err(|e| format!("manifest read: {e}"))?;
        match res.status().as_u16() {
            200 => Ok(Some(
                res.bytes()
                    .await
                    .map_err(|e| format!("manifest read: {e}"))?
                    .to_vec(),
            )),
            404 => Ok(None),
            status => Err(format!("manifest read refused ({status})")),
        }
    }
}

/// The keys of a `ListObjectsV2` page, and the token of the next one.
fn listed(xml: &str) -> (Vec<String>, Option<String>) {
    let between = |s: &str, open: &str, close: &str| -> Vec<String> {
        s.split(open)
            .skip(1)
            .filter_map(|p| p.split(close).next())
            .map(|v| {
                v.replace("&amp;", "&")
                    .replace("&lt;", "<")
                    .replace("&gt;", ">")
            })
            .collect()
    };
    let keys = between(xml, "<Key>", "</Key>");
    let truncated = xml.contains("<IsTruncated>true</IsTruncated>");
    let next = between(xml, "<NextContinuationToken>", "</NextContinuationToken>")
        .into_iter()
        .next()
        .filter(|_| truncated);
    (keys, next)
}

impl TableLookup {
    /// Deletes `namespace.table`, whose files are under `prefix`
    /// (`tables/<uuid>`), with credentials issued within `policy`; then the
    /// namespace, if nothing is left in it. Run again, it finds no file and
    /// drops what is left.
    pub async fn drop(
        &self,
        warehouse: &str,
        (namespace, table): (&str, &str),
        prefix: &str,
        policy: &str,
    ) -> Result<(), String> {
        let creds = self.credentials("drop", policy).await?;
        let purges = self.issuer().properties().drop_purges_files;
        if !purges {
            self.delete_files(&creds, warehouse, prefix).await?;
        }
        let ns = format!(
            "{}/namespaces/{}",
            self.catalog_base(&creds, warehouse).await?,
            encode(namespace, false)
        );
        let url = format!("{ns}/tables/{}", encode(table, false));
        let purge: &[(&str, &str)] = if purges {
            &[("purgeRequested", "true")]
        } else {
            &[]
        };
        let res = self
            .send_as(&creds, "DELETE", &url, purge, Vec::new(), true)
            .await?;
        if !matches!(res.status().as_u16(), 200 | 204 | 404) {
            return Err(format!(
                "catalog refused the drop ({})",
                res.status().as_u16()
            ));
        }
        // Another table still in it: the namespace stays (409).
        let res = self
            .send_as(&creds, "DELETE", &ns, &[], Vec::new(), true)
            .await?;
        match res.status().as_u16() {
            200 | 204 | 404 | 409 => Ok(()),
            status => Err(format!("catalog refused the namespace ({status})")),
        }
    }

    /// Every object under `prefix/` of `warehouse`, listed page by page.
    async fn delete_files(
        &self,
        creds: &crate::lake_sts::TemporaryCredentials,
        warehouse: &str,
        prefix: &str,
    ) -> Result<(), String> {
        let properties = self.issuer().properties().clone();
        let bucket = object_url(&properties, warehouse, "");
        let listing = format!("{}/", prefix.trim_end_matches('/'));
        let mut token: Option<String> = None;
        loop {
            let mut query = vec![("list-type", "2"), ("prefix", listing.as_str())];
            if let Some(t) = &token {
                query.push(("continuation-token", t.as_str()));
            }
            let res = self
                .send_as(creds, "GET", &bucket, &query, Vec::new(), false)
                .await?;
            if res.status().as_u16() != 200 {
                return Err(format!(
                    "lake refused the listing ({})",
                    res.status().as_u16()
                ));
            }
            let xml = res.text().await.map_err(|e| format!("lake: {e}"))?;
            let (keys, next) = listed(&xml);
            for key in keys.iter().filter(|k| k.starts_with(&listing)) {
                let url = object_url(&properties, warehouse, key);
                let res = self
                    .send_as(creds, "DELETE", &url, &[], Vec::new(), false)
                    .await?;
                if !matches!(res.status().as_u16(), 200 | 204 | 404) {
                    return Err(format!("lake refused a file ({})", res.status().as_u16()));
                }
            }
            match next {
                Some(t) => token = Some(t),
                None => return Ok(()),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn lake(style: UrlStyle) -> LakeProperties {
        LakeProperties {
            url_style: style,
            ..crate::rustfs_sts::properties("https://lake.test")
        }
    }

    #[test]
    fn objects_are_addressed_as_the_lake_declares() {
        let key = "manifests/a b.json";
        assert_eq!(
            object_url(&lake(UrlStyle::Path), "qd-w", key),
            "https://lake.test/qd-w/manifests/a%20b.json"
        );
        assert_eq!(
            object_url(&lake(UrlStyle::VirtualHosted), "qd-w", key),
            "https://qd-w.lake.test/manifests/a%20b.json"
        );
    }

    #[test]
    fn a_put_is_signed_as_botocore_signs_it() {
        // botocore's S3SigV4Auth("s3", "us-east-1") on the same request.
        let path = format!(
            "/qd-tenant-a/{}",
            encode("manifests/loans/2021-03-10/evaluate/j.json", true)
        );
        let hash = payload_hash(b"{}");
        let now = Utc.with_ymd_and_hms(2026, 10, 31, 12, 0, 0).unwrap();
        let signed = sigv4::sign(
            "s3",
            "AKID",
            "SECRET",
            "PUT",
            &path,
            &[
                ("content-type", "application/json"),
                ("host", "lake.example"),
                ("if-none-match", "*"),
                ("x-amz-content-sha256", &hash),
                ("x-amz-security-token", "TOKEN"),
            ],
            now,
        );
        assert_eq!(
            signed.authorization,
            "AWS4-HMAC-SHA256 Credential=AKID/20261031/us-east-1/s3/aws4_request, \
             SignedHeaders=content-type;host;if-none-match;x-amz-content-sha256;x-amz-date;\
             x-amz-security-token, \
             Signature=7627a6e111224185756b4daff5357a350b973b5d145c484a792d066f7b9a7cec"
        );
    }

    #[test]
    fn a_listing_gives_its_keys_and_its_next_page() {
        let xml = "<ListBucketResult><IsTruncated>true</IsTruncated>\
            <Contents><Key>tables/a/data/x&amp;y.parquet</Key></Contents>\
            <Contents><Key>tables/a/metadata/v1.json</Key></Contents>\
            <NextContinuationToken>t2</NextContinuationToken></ListBucketResult>";
        let (keys, next) = listed(xml);
        assert_eq!(
            keys,
            ["tables/a/data/x&y.parquet", "tables/a/metadata/v1.json"]
        );
        assert_eq!(next.as_deref(), Some("t2"));
        let (keys, next) =
            listed("<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>");
        assert!(keys.is_empty() && next.is_none());
    }
}
