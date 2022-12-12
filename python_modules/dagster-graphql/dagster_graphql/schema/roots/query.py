from typing import Dict, Mapping, Optional, Sequence, cast

import graphene
from dagster_graphql.implementation.fetch_logs import get_captured_log_metadata
from dagster_graphql.implementation.fetch_runs import get_assets_latest_info

import dagster._check as check
from dagster._core.definitions.events import AssetKey
from dagster._core.execution.backfill import BulkActionStatus
from dagster._core.host_representation import (
    InstigatorSelector,
    RepositorySelector,
    ScheduleSelector,
    SensorSelector,
)
from dagster._core.scheduler.instigation import InstigatorType

from ...implementation.external import (
    fetch_location_statuses,
    fetch_repositories,
    fetch_repository,
    fetch_workspace,
)
from ...implementation.fetch_assets import (
    get_asset,
    get_asset_node,
    get_asset_node_definition_collisions,
    get_asset_nodes,
    get_assets,
    unique_repos,
)
from ...implementation.fetch_backfills import get_backfill, get_backfills
from ...implementation.fetch_instigators import (
    get_instigator_state_or_error,
    get_unloadable_instigator_states_or_error,
)
from ...implementation.fetch_partition_sets import get_partition_set, get_partition_sets_or_error
from ...implementation.fetch_pipelines import (
    get_pipeline_or_error,
    get_pipeline_snapshot_or_error_from_pipeline_selector,
    get_pipeline_snapshot_or_error_from_snapshot_id,
)
from ...implementation.fetch_runs import (
    get_execution_plan,
    get_logs_for_run,
    get_run_by_id,
    get_run_group,
    get_run_groups,
    get_run_tags,
    validate_pipeline_config,
)
from ...implementation.fetch_schedules import (
    get_schedule_or_error,
    get_scheduler_or_error,
    get_schedules_or_error,
)
from ...implementation.fetch_sensors import get_sensor_or_error, get_sensors_or_error
from ...implementation.fetch_solids import get_graph_or_error
from ...implementation.loader import (
    BatchMaterializationLoader,
    CrossRepoAssetDependedByLoader,
    ProjectedLogicalVersionLoader,
)
from ...implementation.run_config_schema import resolve_run_config_schema_or_error
from ...implementation.utils import graph_selector_from_graphql, pipeline_selector_from_graphql
from ..asset_graph import (
    GrapheneAssetLatestInfo,
    GrapheneAssetNode,
    GrapheneAssetNodeDefinitionCollision,
    GrapheneAssetNodeOrError,
)
from ..backfill import (
    GrapheneBulkActionStatus,
    GraphenePartitionBackfillOrError,
    GraphenePartitionBackfillsOrError,
)
from ..external import (
    GrapheneRepositoriesOrError,
    GrapheneRepositoryConnection,
    GrapheneRepositoryOrError,
    GrapheneWorkspaceLocationStatusEntriesOrError,
    GrapheneWorkspaceOrError,
)
from ..inputs import (
    GrapheneAssetGroupSelector,
    GrapheneAssetKeyInput,
    GrapheneGraphSelector,
    GrapheneInstigationSelector,
    GraphenePipelineSelector,
    GrapheneRepositorySelector,
    GrapheneRunsFilter,
    GrapheneScheduleSelector,
    GrapheneSensorSelector,
)
from ..instance import GrapheneInstance
from ..instigation import (
    GrapheneInstigationStateOrError,
    GrapheneInstigationStatesOrError,
    GrapheneInstigationType,
)
from ..logs.compute_logs import (
    GrapheneCapturedLogs,
    GrapheneCapturedLogsMetadata,
    from_captured_log_data,
)
from ..partition_sets import GraphenePartitionSetOrError, GraphenePartitionSetsOrError
from ..permissions import GraphenePermission
from ..pipelines.config_result import GraphenePipelineConfigValidationResult
from ..pipelines.pipeline import GrapheneEventConnectionOrError, GrapheneRunOrError
from ..pipelines.snapshot import GraphenePipelineSnapshotOrError
from ..run_config import GrapheneRunConfigSchemaOrError
from ..runs import (
    GrapheneRunConfigData,
    GrapheneRunGroupOrError,
    GrapheneRunGroupsOrError,
    GrapheneRuns,
    GrapheneRunsOrError,
    parse_run_config_input,
)
from ..schedules import GrapheneScheduleOrError, GrapheneSchedulerOrError, GrapheneSchedulesOrError
from ..sensors import GrapheneSensorOrError, GrapheneSensorsOrError
from ..tags import GraphenePipelineTagAndValues
from ..util import InputObject, ResolveInfo, get_compute_log_manager, non_null_list
from .assets import GrapheneAssetOrError, GrapheneAssetsOrError
from .execution_plan import GrapheneExecutionPlanOrError
from .pipeline import GrapheneGraphOrError, GraphenePipelineOrError


class GrapheneDagitQuery(graphene.ObjectType):
    """The root for all queries to retrieve data from the Dagster instance."""

    class Meta:
        name = "DagitQuery"

    version = graphene.Field(
        graphene.NonNull(graphene.String),
        description="Retrieve the version of Dagster running in the Dagster deployment.",
    )

    repositoriesOrError = graphene.Field(
        graphene.NonNull(GrapheneRepositoriesOrError),
        repositorySelector=graphene.Argument(GrapheneRepositorySelector),
        description="Retrieve all the repositories.",
    )

    repositoryOrError = graphene.Field(
        graphene.NonNull(GrapheneRepositoryOrError),
        repositorySelector=graphene.NonNull(GrapheneRepositorySelector),
        description="Retrieve a repository by its location name and repository name.",
    )

    workspaceOrError = graphene.Field(
        graphene.NonNull(GrapheneWorkspaceOrError),
        description="Retrieve the workspace and its locations.",
    )

    locationStatusesOrError = graphene.Field(
        graphene.NonNull(GrapheneWorkspaceLocationStatusEntriesOrError),
        description="Retrieve location status for workspace locations",
    )

    pipelineOrError = graphene.Field(
        graphene.NonNull(GraphenePipelineOrError),
        params=graphene.NonNull(GraphenePipelineSelector),
        description="Retrieve a job by its location name, repository name, and job name.",
    )

    pipelineSnapshotOrError = graphene.Field(
        graphene.NonNull(GraphenePipelineSnapshotOrError),
        snapshotId=graphene.String(),
        activePipelineSelector=graphene.Argument(GraphenePipelineSelector),
        description=(
            "Retrieve a job snapshot by its id or location name, repository name, and job name."
        ),
    )

    graphOrError = graphene.Field(
        graphene.NonNull(GrapheneGraphOrError),
        selector=graphene.Argument(GrapheneGraphSelector),
        description="Retrieve a graph by its location name, repository name, and graph name.",
    )

    scheduler = graphene.Field(
        graphene.NonNull(GrapheneSchedulerOrError),
        description="Retrieve the name of the scheduler running in the Dagster deployment.",
    )

    scheduleOrError = graphene.Field(
        graphene.NonNull(GrapheneScheduleOrError),
        schedule_selector=graphene.NonNull(GrapheneScheduleSelector),
        description="Retrieve a schedule by its location name, repository name, and schedule name.",
    )

    schedulesOrError = graphene.Field(
        graphene.NonNull(GrapheneSchedulesOrError),
        repositorySelector=graphene.NonNull(GrapheneRepositorySelector),
        description="Retrieve all the schedules.",
    )

    sensorOrError = graphene.Field(
        graphene.NonNull(GrapheneSensorOrError),
        sensorSelector=graphene.NonNull(GrapheneSensorSelector),
        description="Retrieve a sensor by its location name, repository name, and sensor name.",
    )
    sensorsOrError = graphene.Field(
        graphene.NonNull(GrapheneSensorsOrError),
        repositorySelector=graphene.NonNull(GrapheneRepositorySelector),
        description="Retrieve all the sensors.",
    )

    instigationStateOrError = graphene.Field(
        graphene.NonNull(GrapheneInstigationStateOrError),
        instigationSelector=graphene.NonNull(GrapheneInstigationSelector),
        description=(
            "Retrieve the state for a schedule or sensor by its location name, repository name, and"
            " schedule/sensor name."
        ),
    )

    unloadableInstigationStatesOrError = graphene.Field(
        graphene.NonNull(GrapheneInstigationStatesOrError),
        instigationType=graphene.Argument(GrapheneInstigationType),
        description=(
            "Retrieve the running schedules and sensors that are missing from the workspace."
        ),
    )

    partitionSetsOrError = graphene.Field(
        graphene.NonNull(GraphenePartitionSetsOrError),
        repositorySelector=graphene.NonNull(GrapheneRepositorySelector),
        pipelineName=graphene.NonNull(graphene.String),
        description=(
            "Retrieve the partition sets for a job by its location name, repository name, and job"
            " name."
        ),
    )
    partitionSetOrError = graphene.Field(
        graphene.NonNull(GraphenePartitionSetOrError),
        repositorySelector=graphene.NonNull(GrapheneRepositorySelector),
        partitionSetName=graphene.String(),
        description=(
            "Retrieve a partition set by its location name, repository name, and partition set"
            " name."
        ),
    )

    pipelineRunsOrError = graphene.Field(
        graphene.NonNull(GrapheneRunsOrError),
        filter=graphene.Argument(GrapheneRunsFilter),
        cursor=graphene.String(),
        limit=graphene.Int(),
        description="Retrieve runs after applying a filter, cursor, and limit.",
    )
    pipelineRunOrError = graphene.Field(
        graphene.NonNull(GrapheneRunOrError),
        runId=graphene.NonNull(graphene.ID),
        description="Retrieve a run by its run id.",
    )
    runsOrError = graphene.Field(
        graphene.NonNull(GrapheneRunsOrError),
        filter=graphene.Argument(GrapheneRunsFilter),
        cursor=graphene.String(),
        limit=graphene.Int(),
        description="Retrieve runs after applying a filter, cursor, and limit.",
    )
    runOrError = graphene.Field(
        graphene.NonNull(GrapheneRunOrError),
        runId=graphene.NonNull(graphene.ID),
        description="Retrieve a run by its run id.",
    )
    pipelineRunTags = graphene.Field(
        non_null_list(GraphenePipelineTagAndValues),
        description="Retrieve all the distinct key-value tags from all runs.",
    )

    runGroupOrError = graphene.Field(
        graphene.NonNull(GrapheneRunGroupOrError),
        runId=graphene.NonNull(graphene.ID),
        description="Retrieve a group of runs with the matching root run id.",
    )

    runGroupsOrError = graphene.Field(
        graphene.NonNull(GrapheneRunGroupsOrError),
        filter=graphene.Argument(GrapheneRunsFilter),
        cursor=graphene.String(),
        limit=graphene.Int(),
        description="Retrieve groups of runs after applying a filter, cursor, and limit.",
    )

    isPipelineConfigValid = graphene.Field(
        graphene.NonNull(GraphenePipelineConfigValidationResult),
        pipeline=graphene.Argument(graphene.NonNull(GraphenePipelineSelector)),
        mode=graphene.Argument(graphene.NonNull(graphene.String)),
        runConfigData=graphene.Argument(GrapheneRunConfigData),
        description="Retrieve whether the run configuration is valid or invalid.",
    )

    executionPlanOrError = graphene.Field(
        graphene.NonNull(GrapheneExecutionPlanOrError),
        pipeline=graphene.Argument(graphene.NonNull(GraphenePipelineSelector)),
        mode=graphene.Argument(graphene.NonNull(graphene.String)),
        runConfigData=graphene.Argument(GrapheneRunConfigData),
        description="Retrieve the execution plan for a job and its run configuration.",
    )

    runConfigSchemaOrError = graphene.Field(
        graphene.NonNull(GrapheneRunConfigSchemaOrError),
        args={
            "selector": graphene.Argument(graphene.NonNull(GraphenePipelineSelector)),
            "mode": graphene.Argument(graphene.String),
        },
        description="Retrieve the run configuration schema for a job.",
    )

    instance = graphene.Field(
        graphene.NonNull(GrapheneInstance),
        description="Retrieve the instance configuration for the Dagster deployment.",
    )

    assetsOrError = graphene.Field(
        graphene.NonNull(GrapheneAssetsOrError),
        prefix=graphene.List(graphene.NonNull(graphene.String)),
        cursor=graphene.String(),
        limit=graphene.Int(),
        description="Retrieve assets after applying a prefix filter, cursor, and limit.",
    )

    assetOrError = graphene.Field(
        graphene.NonNull(GrapheneAssetOrError),
        assetKey=graphene.Argument(graphene.NonNull(GrapheneAssetKeyInput)),
        description="Retrieve an asset by asset key.",
    )

    assetNodes = graphene.Field(
        non_null_list(GrapheneAssetNode),
        group=graphene.Argument(GrapheneAssetGroupSelector),
        pipeline=graphene.Argument(GraphenePipelineSelector),
        assetKeys=graphene.Argument(graphene.List(graphene.NonNull(GrapheneAssetKeyInput))),
        loadMaterializations=graphene.Boolean(default_value=False),
        description=(
            "Retrieve asset nodes after applying a filter on asset group, job, and asset keys."
        ),
    )

    assetNodeOrError = graphene.Field(
        graphene.NonNull(GrapheneAssetNodeOrError),
        assetKey=graphene.Argument(graphene.NonNull(GrapheneAssetKeyInput)),
        description="Retrieve an asset node by asset key.",
    )

    assetNodeDefinitionCollisions = graphene.Field(
        non_null_list(GrapheneAssetNodeDefinitionCollision),
        assetKeys=graphene.Argument(graphene.List(graphene.NonNull(GrapheneAssetKeyInput))),
        description=(
            "Retrieve a list of asset keys where two or more repos provide an asset definition."
            " Note: Assets should "
        )
        + "not be defined in more than one repository - this query is used to present warnings and"
        " errors in Dagit.",
    )

    partitionBackfillOrError = graphene.Field(
        graphene.NonNull(GraphenePartitionBackfillOrError),
        backfillId=graphene.Argument(graphene.NonNull(graphene.String)),
        description="Retrieve a backfill by backfill id.",
    )

    partitionBackfillsOrError = graphene.Field(
        graphene.NonNull(GraphenePartitionBackfillsOrError),
        status=graphene.Argument(GrapheneBulkActionStatus),
        cursor=graphene.String(),
        limit=graphene.Int(),
        description="Retrieve backfills after applying a status filter, cursor, and limit.",
    )

    permissions = graphene.Field(
        non_null_list(GraphenePermission),
        description="Retrieve the set of permissions for the Dagster deployment.",
    )

    assetsLatestInfo = graphene.Field(
        non_null_list(GrapheneAssetLatestInfo),
        assetKeys=graphene.Argument(non_null_list(GrapheneAssetKeyInput)),
        description="Retrieve the latest materializations for a set of assets by asset keys.",
    )

    logsForRun = graphene.Field(
        graphene.NonNull(GrapheneEventConnectionOrError),
        runId=graphene.NonNull(graphene.ID),
        afterCursor=graphene.String(),
        limit=graphene.Int(),
        description="Retrieve event logs after applying a run id filter, cursor, and limit.",
    )

    capturedLogsMetadata = graphene.Field(
        graphene.NonNull(GrapheneCapturedLogsMetadata),
        logKey=graphene.Argument(non_null_list(graphene.String)),
        description="Retrieve the captured log metadata for a given log key.",
    )
    capturedLogs = graphene.Field(
        graphene.NonNull(GrapheneCapturedLogs),
        logKey=graphene.Argument(non_null_list(graphene.String)),
        cursor=graphene.Argument(graphene.String),
        limit=graphene.Argument(graphene.Int),
        description="Captured logs are the stdout/stderr logs for a given log key",
    )

    def resolve_repositoriesOrError(
        self, graphene_info: ResolveInfo, repositorySelector: Optional[InputObject] = None
    ):
        if repositorySelector:
            return GrapheneRepositoryConnection(
                nodes=[
                    fetch_repository(
                        graphene_info,
                        RepositorySelector.from_graphql_input(repositorySelector),
                    )
                ]
            )
        return fetch_repositories(graphene_info)

    def resolve_repositoryOrError(
        self, graphene_info: ResolveInfo, repositorySelector: InputObject
    ):
        return fetch_repository(
            graphene_info, RepositorySelector.from_graphql_input(repositorySelector)
        )

    def resolve_workspaceOrError(self, graphene_info: ResolveInfo):
        return fetch_workspace(graphene_info.context)

    def resolve_locationStatusesOrError(self, graphene_info: ResolveInfo):
        return fetch_location_statuses(graphene_info.context)

    def resolve_pipelineSnapshotOrError(
        self,
        graphene_info: ResolveInfo,
        snapshotId: Optional[str] = None,
        activePipelineSelector: Optional[InputObject] = None,
    ):
        check.invariant(
            not (snapshotId and activePipelineSelector),
            "Must only pass one of snapshotId or activePipelineSelector",
        )
        check.invariant(
            snapshotId or activePipelineSelector,
            "Must set one of snapshotId or activePipelineSelector",
        )

        if activePipelineSelector:
            pipeline_selector = pipeline_selector_from_graphql(activePipelineSelector)
            return get_pipeline_snapshot_or_error_from_pipeline_selector(
                graphene_info, pipeline_selector
            )
        else:
            return get_pipeline_snapshot_or_error_from_snapshot_id(graphene_info, snapshotId)

    def resolve_graphOrError(
        self, graphene_info: ResolveInfo, selector: Optional[InputObject] = None
    ):
        assert selector is not None
        graph_selector = graph_selector_from_graphql(selector)
        return get_graph_or_error(graphene_info, graph_selector)

    def resolve_version(self, graphene_info: ResolveInfo):
        return graphene_info.context.version

    def resolve_scheduler(self, graphene_info: ResolveInfo):
        return get_scheduler_or_error(graphene_info)

    def resolve_scheduleOrError(self, graphene_info: ResolveInfo, schedule_selector):
        return get_schedule_or_error(
            graphene_info, ScheduleSelector.from_graphql_input(schedule_selector)
        )

    def resolve_schedulesOrError(self, graphene_info: ResolveInfo, repositorySelector: InputObject):
        return get_schedules_or_error(
            graphene_info,
            RepositorySelector.from_graphql_input(repositorySelector),
        )

    def resolve_sensorOrError(self, graphene_info: ResolveInfo, sensorSelector: InputObject):
        return get_sensor_or_error(graphene_info, SensorSelector.from_graphql_input(sensorSelector))

    def resolve_sensorsOrError(self, graphene_info: ResolveInfo, repositorySelector: InputObject):
        return get_sensors_or_error(
            graphene_info,
            RepositorySelector.from_graphql_input(repositorySelector),
        )

    def resolve_instigationStateOrError(
        self, graphene_info: ResolveInfo, instigationSelector: InputObject
    ):
        return get_instigator_state_or_error(
            graphene_info, InstigatorSelector.from_graphql_input(instigationSelector)
        )

    def resolve_unloadableInstigationStatesOrError(
        self, graphene_info: ResolveInfo, instigationType: Optional[GrapheneInstigationType] = None
    ):
        instigation_type = InstigatorType(instigationType) if instigationType else None
        return get_unloadable_instigator_states_or_error(graphene_info, instigation_type)

    def resolve_pipelineOrError(self, graphene_info: ResolveInfo, params: InputObject):
        return get_pipeline_or_error(
            graphene_info,
            pipeline_selector_from_graphql(params),
        )

    def resolve_pipelineRunsOrError(
        self,
        _graphene_info: ResolveInfo,
        filter: Optional[InputObject] = None,
        cursor: Optional[str] = None,
        limit: Optional[int] = None,
    ):
        if filter is not None:
            filter = filter.to_selector()

        return GrapheneRuns(
            filters=filter,
            cursor=cursor,
            limit=limit,
        )

    def resolve_pipelineRunOrError(self, graphene_info: ResolveInfo, runId):
        return get_run_by_id(graphene_info, runId)

    def resolve_runsOrError(
        self,
        _graphene_info: ResolveInfo,
        filter: Optional[InputObject] = None,
        cursor: Optional[str] = None,
        limit: Optional[int] = None,
    ):
        if filter is not None:
            filter = filter.to_selector()

        return GrapheneRuns(
            filters=filter,
            cursor=cursor,
            limit=limit,
        )

    def resolve_runOrError(self, graphene_info: ResolveInfo, runId):
        return get_run_by_id(graphene_info, runId)

    def resolve_runGroupsOrError(
        self,
        graphene_info: ResolveInfo,
        filter: Optional[InputObject] = None,
        cursor: Optional[str] = None,
        limit: Optional[int] = None,
    ):
        if filter is not None:
            filter = filter.to_selector()

        return GrapheneRunGroupsOrError(
            results=get_run_groups(graphene_info, filter, cursor, limit)
        )

    def resolve_partitionSetsOrError(
        self, graphene_info: ResolveInfo, repositorySelector: RepositorySelector, pipelineName: str
    ):
        return get_partition_sets_or_error(
            graphene_info,
            RepositorySelector.from_graphql_input(repositorySelector),
            pipelineName,
        )

    def resolve_partitionSetOrError(
        self,
        graphene_info: ResolveInfo,
        repositorySelector: RepositorySelector,
        partitionSetName: Optional[str] = None,
    ):
        return get_partition_set(
            graphene_info,
            RepositorySelector.from_graphql_input(repositorySelector),
            check.not_none(partitionSetName),
        )

    def resolve_pipelineRunTags(self, graphene_info: ResolveInfo):
        return get_run_tags(graphene_info)

    def resolve_runGroupOrError(self, graphene_info: ResolveInfo, runId):
        return get_run_group(graphene_info, runId)

    def resolve_isPipelineConfigValid(
        self,
        graphene_info: ResolveInfo,
        pipeline: InputObject,
        mode: str,
        runConfigData: Optional[InputObject] = None,
    ):
        return validate_pipeline_config(
            graphene_info,
            pipeline_selector_from_graphql(pipeline),
            parse_run_config_input(runConfigData or {}, raise_on_error=False),
            mode,
        )

    def resolve_executionPlanOrError(
        self,
        graphene_info: ResolveInfo,
        pipeline: InputObject,
        mode: str,
        runConfigData: Optional[InputObject] = None,
    ):
        return get_execution_plan(
            graphene_info,
            pipeline_selector_from_graphql(pipeline),
            parse_run_config_input(runConfigData or {}, raise_on_error=True),
            mode,
        )

    def resolve_runConfigSchemaOrError(
        self, graphene_info: ResolveInfo, selector: InputObject, mode: Optional[str] = None
    ):
        return resolve_run_config_schema_or_error(
            graphene_info,
            pipeline_selector_from_graphql(selector),
            mode,
        )

    def resolve_instance(self, graphene_info: ResolveInfo):
        return GrapheneInstance(graphene_info.context.instance)

    def resolve_assetNodes(
        self,
        graphene_info: ResolveInfo,
        load_materializations: bool,
        group: Optional[GrapheneAssetGroupSelector] = None,
        pipeline: Optional[GraphenePipelineSelector] = None,
        asset_keys: Optional[Sequence[GrapheneAssetKeyInput]] = None,
    ) -> Sequence[GrapheneAssetNode]:
        resolved_asset_keys = set(
            AssetKey.from_graphql_input(cast(Mapping[str, Sequence[str]], asset_key_input))
            for asset_key_input in asset_keys or []
        )
        use_all_asset_keys = len(resolved_asset_keys) == 0

        repo = None
        if group is not None:
            group_name = group.groupName
            repo_sel = RepositorySelector.from_graphql_input(group)
            repo_loc = graphene_info.context.get_repository_location(repo_sel.location_name)
            repo = repo_loc.get_repository(repo_sel.repository_name)
            external_asset_nodes = repo.get_external_asset_nodes()
            results = (
                [
                    GrapheneAssetNode(repo_loc, repo, asset_node)
                    for asset_node in external_asset_nodes
                    if asset_node.group_name == group_name
                ]
                if external_asset_nodes
                else []
            )
        elif pipeline is not None:
            pipeline_name = pipeline.pipelineName
            repo_sel = RepositorySelector.from_graphql_input(pipeline)
            repo_loc = graphene_info.context.get_repository_location(repo_sel.location_name)
            repo = repo_loc.get_repository(repo_sel.repository_name)
            external_asset_nodes = repo.get_external_asset_nodes(pipeline_name)
            results = (
                [
                    GrapheneAssetNode(repo_loc, repo, asset_node)
                    for asset_node in external_asset_nodes
                ]
                if external_asset_nodes
                else []
            )
        else:
            results = get_asset_nodes(graphene_info)

        # Filter down to requested asset keys
        results = [
            node for node in results if use_all_asset_keys or node.assetKey in resolved_asset_keys
        ]

        if not results:
            return []

        materialization_loader = BatchMaterializationLoader(
            instance=graphene_info.context.instance,
            asset_keys=[node.assetKey for node in results],
        )

        depended_by_loader = CrossRepoAssetDependedByLoader(context=graphene_info.context)

        if repo is not None:
            repos = [repo]
        else:
            repos = unique_repos(result.external_repository for result in results)

        projected_logical_version_loader = ProjectedLogicalVersionLoader(
            instance=graphene_info.context.instance,
            key_to_node_map={node.assetKey: node.external_asset_node for node in results},
            repositories=repos,
        )

        return [
            GrapheneAssetNode(
                node.repository_location,
                node.external_repository,
                node.external_asset_node,
                materialization_loader=materialization_loader,
                depended_by_loader=depended_by_loader,
                projected_logical_version_loader=projected_logical_version_loader,
            )
            for node in results
        ]

    def resolve_assetNodeOrError(self, graphene_info: ResolveInfo, assetKey: InputObject):
        asset_key_input = cast(Mapping[str, Sequence[str]], assetKey)
        return get_asset_node(graphene_info, AssetKey.from_graphql_input(asset_key_input))

    def resolve_assetsOrError(
        self,
        graphene_info: ResolveInfo,
        prefix: Optional[Sequence[str]] = None,
        cursor: Optional[str] = None,
        limit: Optional[int] = None,
    ):
        return get_assets(
            graphene_info,
            prefix=prefix,
            cursor=cursor,
            limit=limit,
        )

    def resolve_assetOrError(self, graphene_info: ResolveInfo, assetKey: InputObject):
        return get_asset(graphene_info, AssetKey.from_graphql_input(assetKey))

    def resolve_assetNodeDefinitionCollisions(
        self, graphene_info: ResolveInfo, assetKeys: Optional[Sequence[InputObject]] = None
    ):
        assert assetKeys is not None
        raw_asset_keys = cast(Sequence[Mapping[str, Sequence[str]]], assetKeys)
        asset_keys = set(AssetKey.from_graphql_input(asset_key) for asset_key in raw_asset_keys)
        return get_asset_node_definition_collisions(graphene_info, asset_keys)

    def resolve_partitionBackfillOrError(self, graphene_info: ResolveInfo, backfillId: str):
        return get_backfill(graphene_info, backfillId)

    def resolve_partitionBackfillsOrError(
        self,
        graphene_info: ResolveInfo,
        status: Optional[GrapheneBulkActionStatus] = None,
        cursor: Optional[str] = None,
        limit: Optional[int] = None,
    ):
        return get_backfills(
            graphene_info,
            status=BulkActionStatus.from_graphql_input(status) if status else None,
            cursor=cursor,
            limit=limit,
        )

    def resolve_permissions(self, graphene_info: ResolveInfo):
        permissions = graphene_info.context.permissions
        return [GraphenePermission(permission, value) for permission, value in permissions.items()]

    def resolve_assetsLatestInfo(
        self, graphene_info: ResolveInfo, assetKeys: Sequence[InputObject]
    ):
        asset_keys = set(AssetKey.from_graphql_input(asset_key) for asset_key in assetKeys)

        results = get_asset_nodes(graphene_info)

        # Filter down to requested asset keys
        # Build mapping of asset key to the step keys required to generate the asset
        step_keys_by_asset: Dict[AssetKey, Sequence[str]] = {  # type: ignore
            node.external_asset_node.asset_key: node.external_asset_node.op_names
            for node in results
            if node.assetKey in asset_keys
        }

        return get_assets_latest_info(graphene_info, step_keys_by_asset)

    def resolve_logsForRun(
        self,
        graphene_info: ResolveInfo,
        runId: str,
        afterCursor: Optional[str] = None,
        limit: Optional[int] = None,
    ):
        return get_logs_for_run(graphene_info, runId, afterCursor, limit)

    def resolve_capturedLogsMetadata(
        self, graphene_info: ResolveInfo, logKey: Sequence[str]
    ) -> GrapheneCapturedLogsMetadata:
        return get_captured_log_metadata(graphene_info, logKey)

    def resolve_capturedLogs(
        self,
        graphene_info: ResolveInfo,
        logKey: Sequence[str],
        cursor: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> GrapheneCapturedLogs:
        # Type-ignore because `get_log_data` returns a `ComputeLogManager` but in practice this is
        # always also an instance of `CapturedLogManager`, which defines `get_log_data`. Probably
        # `ComputeLogManager` should subclass `CapturedLogManager`.
        log_data = get_compute_log_manager(graphene_info).get_log_data(
            logKey, cursor=cursor, max_bytes=limit
        )
        return from_captured_log_data(log_data)
