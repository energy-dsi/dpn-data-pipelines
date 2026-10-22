# Copyright 2026 DSI Project
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
Pins down the proposed-name conventions shown on the create-flow's review
step (service/models.py's default_file_names()/default_topic_names()):

  File pathway storage containers : <product_type>-stage / <product_type>-target
  Topic pathway Kafka topics       : dpn-producer-<product_type>-stage /
                                      dpn-producer-<product_type>-target

Both are overridable on the review screen — these are only the proposed
starting defaults.
"""
from __future__ import annotations

from service.models import default_file_names, default_topic_names


def test_default_file_names_use_stage_target_suffixes():
    names = default_file_names("bp-natural-gas")
    assert names["source_container_name"] == "bp-natural-gas-stage"
    assert names["target_container_name"] == "bp-natural-gas-target"


def test_default_topic_names_use_stage_target_suffixes_with_no_org_component():
    names = default_topic_names("bp-natural-gas")
    assert names["src_topic_name"] == "dpn-producer-bp-natural-gas-stage"
    assert names["target_topic_name"] == "dpn-producer-bp-natural-gas-target"
