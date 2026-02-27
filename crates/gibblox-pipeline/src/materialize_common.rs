use alloc::{string::String, vec::Vec};

use url::Url;

pub fn derive_casync_chunk_store_url(index_url: &Url) -> Result<Url, url::ParseError> {
    if let Some(segments) = index_url.path_segments() {
        let segments: Vec<&str> = segments.collect();
        if let Some(index_pos) = segments.iter().rposition(|segment| *segment == "indexes") {
            let mut base_segments = segments[..=index_pos].to_vec();
            base_segments[index_pos] = "chunks";

            let mut url = index_url.clone();
            let mut path = String::from("/");
            path.push_str(base_segments.join("/").as_str());
            if !path.ends_with('/') {
                path.push('/');
            }
            url.set_path(path.as_str());
            url.set_query(None);
            url.set_fragment(None);
            return Ok(url);
        }
    }

    index_url.join("./")
}
