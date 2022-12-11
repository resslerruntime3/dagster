import graphene
from typing_extensions import Protocol, TypeAlias
from typing import Any, cast


from dagster._core.storage.captured_log_manager import CapturedLogManager
from dagster._core.workspace.context import WorkspaceRequestContext

# Unfortunately Graphene input objects do not play nicely with typing-- we can't type arguments to
# resolvers with `GrapheneMyInputObjectType` and have it work. We use this `InputObject` type as a
# stand-in until a better solution is found.
InputObject: TypeAlias = Any


# Assign this type to `graphene_info` in a resolver to apply typing to `graphene_info.context`.
class HasContext(Protocol):
    @property
    def context(self) -> WorkspaceRequestContext:
        ...


def non_null_list(of_type):
    return graphene.NonNull(graphene.List(graphene.NonNull(of_type)))
