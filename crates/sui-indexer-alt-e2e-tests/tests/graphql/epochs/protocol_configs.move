// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//# init --protocol-version 70 --accounts A --simulator

//# create-checkpoint

//# run-graphql
{ # Protocol Configs that don't exist (because they haven't been used in the
  # chain being indexed)
  protocolConfigs(version: 69) { protocolVersion }
  protocolConfigs(version: 71) { protocolVersion }
}

//# run-graphql
{
  protocolConfigs(version: 70) {
    config(key: "max_move_object_size") { key value }
    featureFlag(key: "enable_effects_v2") { key value }
  }
}

//# run-graphql
{ # Fetch protocol config version via epoch
  epoch(epochId: 0) { protocolConfigs { protocolVersion } }

  # Fetch protocol config via version
  protocolConfigs(version: 70) {
    protocolVersion
    configs { key value }
    featureFlags { key value }
  }
}
