// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Code generated by sidekick. DO NOT EDIT.

impl gaxi::prost::ToProto<LatLng> for gtype::model::LatLng {
    type Output = LatLng;
    fn to_proto(self) -> std::result::Result<LatLng, gaxi::prost::ConvertError> {
        Ok(Self::Output {
            latitude: self.latitude.to_proto()?,
            longitude: self.longitude.to_proto()?,
        })
    }
}

impl gaxi::prost::FromProto<gtype::model::LatLng> for LatLng {
    fn cnv(self) -> std::result::Result<gtype::model::LatLng, gaxi::prost::ConvertError> {
        Ok(
            gtype::model::LatLng::new()
                .set_latitude(self.latitude)
                .set_longitude(self.longitude)
        )
    }
}
