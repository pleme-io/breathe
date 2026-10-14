use breathe_control::Unit;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

const KI: u64 = 1024;
const MI: u64 = KI * 1024;
const GI: u64 = MI * 1024;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Bytes(pub u64);

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Millicores(pub u64);

impl Bytes {
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        Unit::Bytes.parse(s).map(Self)
    }

    #[must_use]
    pub fn render(self) -> String {
        let v = self.0;
        if v != 0 && v.is_multiple_of(GI) {
            format!("{}Gi", v / GI)
        } else if v != 0 && v.is_multiple_of(MI) {
            format!("{}Mi", v / MI)
        } else if v != 0 && v.is_multiple_of(KI) {
            format!("{}Ki", v / KI)
        } else {
            v.to_string()
        }
    }

    #[must_use]
    pub fn round_up_mi(self) -> Self {
        Self(self.0.div_ceil(MI) * MI)
    }
}

impl Millicores {
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        Unit::Millicores.parse(s).map(Self)
    }

    #[must_use]
    pub fn render(self) -> String {
        format!("{}m", self.0)
    }

    #[must_use]
    pub fn round_up_10(self) -> Self {
        Self(self.0.div_ceil(10) * 10)
    }
}

macro_rules! quantity_serde {
    ($t:ty, $what:literal) => {
        impl Serialize for $t {
            fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                s.serialize_str(&self.render())
            }
        }

        impl<'de> Deserialize<'de> for $t {
            fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
                let v = serde_yaml::Value::deserialize(d)?;
                let s = match &v {
                    serde_yaml::Value::String(s) => s.clone(),
                    serde_yaml::Value::Number(n) => n.to_string(),
                    _ => return Err(serde::de::Error::custom(concat!("expected a ", $what, " quantity"))),
                };
                <$t>::parse(&s).ok_or_else(|| {
                    serde::de::Error::custom(format!(concat!("malformed ", $what, " quantity {:?}"), s))
                })
            }
        }
    };
}

quantity_serde!(Bytes, "memory");
quantity_serde!(Millicores, "cpu");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_the_largest_exact_binary_unit() {
        assert_eq!(Bytes(2 * GI).render(), "2Gi");
        assert_eq!(Bytes(1280 * MI).render(), "1280Mi");
        assert_eq!(Bytes(3).render(), "3");
    }

    #[test]
    fn round_trips_repo_spellings() {
        for s in ["640Mi", "4Gi", "16Gi", "1376Mi"] {
            assert_eq!(Bytes::parse(s).unwrap().render(), s);
        }
        assert_eq!(Millicores::parse("3500m").unwrap(), Millicores(3500));
        assert_eq!(Millicores::parse("2").unwrap(), Millicores(2000));
    }
}
