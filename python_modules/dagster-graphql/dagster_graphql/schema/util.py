from typing import Any, cast

import graphene
from typing_extensions import TypeAlias

from dagster._core.storage.captured_log_manager import CapturedLogManager
from dagster._core.workspace.context import WorkspaceRequestContext

# Unfortunately Graphene input objects do not play nicely with typing-- we can't type arguments to
# resolvers with `GrapheneMyInputObjectType` and have it work. We use this `InputObject` type as a
# stand-in until a better solution is found.
InputObject: TypeAlias = Any


class ResolveInfo(graphene.ResolveInfo):
    @property
    def context(self) -> WorkspaceRequestContext:
        return cast(WorkspaceRequestContext, super().context)


def non_null_list(of_type):
    return graphene.NonNull(graphene.List(graphene.NonNull(of_type)))


# Type-ignore because `get_log_data` returns a `ComputeLogManager` but in practice this is
# always also an instance of `CapturedLogManager`, which defines the APIs that we access in
# dagster-graphql. Probably `ComputeLogManager` should subclass `CapturedLogManager`-- this is a
# temporary workaround to satisfy type-checking.
def get_compute_log_manager(graphene_info: ResolveInfo) -> CapturedLogManager:
    return cast(CapturedLogManager, graphene_info.context.instance.compute_log_manager)
