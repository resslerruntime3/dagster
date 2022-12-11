import {gql, useQuery} from '@apollo/client';
import {Box, Colors, ConfigTypeSchema, Icon, Spinner} from '@dagster-io/ui';
import * as React from 'react';
import {Link} from 'react-router-dom';
import styled from 'styled-components/macro';

import {ASSET_NODE_CONFIG_FRAGMENT} from '../assets/AssetConfig';
import {AssetDefinedInMultipleReposNotice} from '../assets/AssetDefinedInMultipleReposNotice';
import {
  AssetMetadataTable,
  ASSET_NODE_OP_METADATA_FRAGMENT,
  metadataForAssetNode,
} from '../assets/AssetMetadata';
import {AssetSidebarActivitySummary} from '../assets/AssetSidebarActivitySummary';
import {DependsOnSelfBanner} from '../assets/DependsOnSelfBanner';
import {PartitionHealthSummary} from '../assets/PartitionHealthSummary';
import {assetDetailsPathForKey} from '../assets/assetDetailsPathForKey';
import {AssetKey} from '../assets/types';
import {usePartitionHealthData} from '../assets/usePartitionHealthData';
import {DagsterTypeSummary} from '../dagstertype/DagsterType';
import {DagsterTypeFragment} from '../dagstertype/types/DagsterTypeFragment';
import {METADATA_ENTRY_FRAGMENT} from '../metadata/MetadataEntry';
import {Description} from '../pipelines/Description';
import {SidebarSection, SidebarTitle} from '../pipelines/SidebarComponents';
import {pluginForMetadata} from '../plugins';
import {Version} from '../versions/Version';
import {buildRepoAddress} from '../workspace/buildRepoAddress';

import {LiveDataForNode, displayNameForAssetKey, GraphNode, nodeDependsOnSelf} from './Utils';
import {SidebarAssetQuery, SidebarAssetQueryVariables} from './types/SidebarAssetQuery';

export const SidebarAssetInfo: React.FC<{
  assetNode: GraphNode;
  liveData: LiveDataForNode;
}> = ({assetNode, liveData}) => {
  const assetKey = assetNode.assetKey;
  const partitionHealthData = usePartitionHealthData([assetKey]);
  const {data} = useQuery<SidebarAssetQuery, SidebarAssetQueryVariables>(SIDEBAR_ASSET_QUERY, {
    variables: {assetKey: {path: assetKey.path}},
    fetchPolicy: 'cache-and-network',
  });

  const {lastMaterialization} = liveData || {};
  const asset = data?.assetNodeOrError.__typename === 'AssetNode' ? data.assetNodeOrError : null;
  if (!asset) {
    return (
      <>
        <Header assetKey={assetKey} />
        <Box padding={{vertical: 64}}>
          <Spinner purpose="section" />
        </Box>
      </>
    );
  }

  const repoAddress = buildRepoAddress(asset.repository.name, asset.repository.location.name);
  const {assetMetadata, assetType} = metadataForAssetNode(asset);
  const hasAssetMetadata = assetType || assetMetadata.length > 0;
  const assetConfigSchema = asset.configField?.configType;

  const OpMetadataPlugin = asset.op?.metadata && pluginForMetadata(asset.op.metadata);

  return (
    <>
      <Header assetKey={assetKey} opName={asset.op?.name} />

      <AssetDefinedInMultipleReposNotice
        assetKey={assetKey}
        loadedFromRepo={repoAddress}
        padded={false}
      />

      <AssetSidebarActivitySummary
        assetKey={assetKey}
        assetLastMaterializedAt={lastMaterialization?.timestamp}
        assetHasDefinedPartitions={!!asset.partitionDefinition}
        liveData={liveData}
      />

      <div style={{borderBottom: `2px solid ${Colors.Gray300}`}} />

      {nodeDependsOnSelf(assetNode) && <DependsOnSelfBanner />}

      {(asset.description || OpMetadataPlugin?.SidebarComponent || !hasAssetMetadata) && (
        <SidebarSection title="Description">
          <Box padding={{vertical: 16, horizontal: 24}}>
            <Description description={asset.description || 'No description provided.'} />
          </Box>
          {asset.op && OpMetadataPlugin?.SidebarComponent && (
            <Box padding={{bottom: 16, horizontal: 24}}>
              <OpMetadataPlugin.SidebarComponent definition={asset.op} repoAddress={repoAddress} />
            </Box>
          )}
        </SidebarSection>
      )}

      {asset.opVersion && (
        <SidebarSection title="Code Version">
          <Box padding={{vertical: 16, horizontal: 24}}>
            <Version>{asset.opVersion}</Version>
          </Box>
        </SidebarSection>
      )}

      {assetConfigSchema && (
        <SidebarSection title="Config">
          <Box padding={{vertical: 16, horizontal: 24}}>
            <ConfigTypeSchema
              type={assetConfigSchema}
              typesInScope={assetConfigSchema.recursiveConfigTypes}
            />
          </Box>
        </SidebarSection>
      )}

      {assetMetadata.length > 0 && (
        <SidebarSection title="Metadata">
          <AssetMetadataTable assetMetadata={assetMetadata} repoLocation={repoAddress?.location} />
        </SidebarSection>
      )}

      {assetType && <TypeSidebarSection assetType={assetType} />}

      {asset.partitionDefinition && (
        <SidebarSection title="Partitions">
          <Box padding={{vertical: 16, horizontal: 24}} flex={{direction: 'column', gap: 16}}>
            <p>{asset.partitionDefinition.description}</p>
            <PartitionHealthSummary assetKey={asset.assetKey} data={partitionHealthData} />
          </Box>
        </SidebarSection>
      )}
    </>
  );
};

const TypeSidebarSection: React.FC<{
  assetType: DagsterTypeFragment;
}> = ({assetType}) => {
  return (
    <SidebarSection title="Type">
      <DagsterTypeSummary type={assetType} />
    </SidebarSection>
  );
};

const Header: React.FC<{assetKey: AssetKey; opName?: string}> = ({assetKey, opName}) => {
  const displayName = displayNameForAssetKey(assetKey);

  return (
    <Box flex={{gap: 4, direction: 'column'}} margin={{left: 24, right: 12, vertical: 16}}>
      <SidebarTitle
        style={{
          marginBottom: 0,
          display: 'flex',
          justifyContent: 'space-between',
          flexWrap: 'wrap',
        }}
      >
        <Box>{displayName}</Box>
        {displayName !== opName ? (
          <Box style={{opacity: 0.5}} flex={{gap: 6, alignItems: 'center'}}>
            <Icon name="op" size={16} />
            {opName}
          </Box>
        ) : undefined}
      </SidebarTitle>
      <AssetCatalogLink to={assetDetailsPathForKey(assetKey)}>
        {'View in Asset Catalog '}
        <Icon name="open_in_new" color={Colors.Link} />
      </AssetCatalogLink>
    </Box>
  );
};
const AssetCatalogLink = styled(Link)`
  display: flex;
  gap: 5px;
  padding: 6px;
  margin: -6px;
  align-items: center;
  white-space: nowrap;
`;

export const SIDEBAR_ASSET_FRAGMENT = gql`
  fragment SidebarAssetFragment on AssetNode {
    id
    description
    ...AssetNodeConfigFragment
    metadataEntries {
      ...MetadataEntryFragment
    }
    partitionDefinition {
      description
    }
    assetKey {
      path
    }
    op {
      name
      description
      metadata {
        key
        value
      }
    }
    opVersion
    repository {
      id
      name
      location {
        id
        name
      }
    }

    ...AssetNodeOpMetadataFragment
  }
  ${ASSET_NODE_CONFIG_FRAGMENT}
  ${ASSET_NODE_OP_METADATA_FRAGMENT}
  ${METADATA_ENTRY_FRAGMENT}
`;

const SIDEBAR_ASSET_QUERY = gql`
  query SidebarAssetQuery($assetKey: AssetKeyInput!) {
    assetNodeOrError(assetKey: $assetKey) {
      __typename
      ... on AssetNode {
        id
        ...SidebarAssetFragment
      }
    }
  }
  ${SIDEBAR_ASSET_FRAGMENT}
`;
