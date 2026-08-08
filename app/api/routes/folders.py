"""Folder routes - creation, recursive listing, move/rename, sub-tree delete."""

from __future__ import annotations

import uuid
from typing import Annotated

from fastapi import APIRouter, Query, status

from app.api.deps import FileServiceDep, FolderServiceDep, OwnerDep, SessionDep
from app.models.folder import Folder
from app.schemas.folder import (
    FolderCreateRequest,
    FolderDeleteResponse,
    FolderListResponse,
    FolderNode,
    FolderResponse,
    FolderUpdateRequest,
)

router = APIRouter(prefix="/folders", tags=["folders"])


def _build_tree(folders: list[Folder], counts: dict[uuid.UUID, int]) -> list[FolderNode]:
    """Assemble the nested tree from a single flat, path-ordered query."""
    nodes: dict[uuid.UUID, FolderNode] = {}
    for folder in folders:
        node = FolderNode.model_validate(folder, from_attributes=True)
        node.children = []
        node.file_count = counts.get(folder.id, 0)
        nodes[folder.id] = node

    roots: list[FolderNode] = []
    for folder in folders:
        node = nodes[folder.id]
        parent = nodes.get(folder.parent_id) if folder.parent_id else None
        if parent is None:
            roots.append(node)
        else:
            parent.children.append(node)
    return roots


@router.post(
    "",
    response_model=FolderResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a folder under a parent (defaults to root)",
)
async def create_folder(
    payload: FolderCreateRequest,
    owner_id: OwnerDep,
    folders: FolderServiceDep,
    session: SessionDep,
) -> FolderResponse:
    folder = await folders.create(owner_id, name=payload.name, parent_id=payload.parent_id)
    await session.commit()
    return FolderResponse.model_validate(folder, from_attributes=True)


@router.get("", response_model=FolderListResponse, summary="Flat list of every folder")
async def list_folders(owner_id: OwnerDep, folders: FolderServiceDep) -> FolderListResponse:
    items = await folders.list_all(owner_id)
    if not items:
        await folders.get_root(owner_id)
        items = await folders.list_all(owner_id)
    return FolderListResponse(
        items=[FolderResponse.model_validate(item, from_attributes=True) for item in items],
        total=len(items),
    )


@router.get("/tree", response_model=list[FolderNode], summary="Nested folder tree")
async def folder_tree(
    owner_id: OwnerDep,
    folders: FolderServiceDep,
    session: SessionDep,
) -> list[FolderNode]:
    root = await folders.get_root(owner_id)
    await session.commit()
    items = await folders.list_subtree(root)
    counts = await folders.file_counts(owner_id)
    return _build_tree(items, counts)


@router.get("/root", response_model=FolderResponse, summary="The owner's root folder")
async def get_root_folder(
    owner_id: OwnerDep,
    folders: FolderServiceDep,
    session: SessionDep,
) -> FolderResponse:
    root = await folders.get_root(owner_id)
    await session.commit()
    return FolderResponse.model_validate(root, from_attributes=True)


@router.get("/{folder_id}", response_model=FolderResponse, summary="Fetch one folder")
async def get_folder(
    folder_id: uuid.UUID,
    owner_id: OwnerDep,
    folders: FolderServiceDep,
) -> FolderResponse:
    folder = await folders.get(folder_id, owner_id)
    return FolderResponse.model_validate(folder, from_attributes=True)


@router.get(
    "/{folder_id}/children",
    response_model=FolderListResponse,
    summary="Immediate sub-folders",
)
async def list_children(
    folder_id: uuid.UUID,
    owner_id: OwnerDep,
    folders: FolderServiceDep,
) -> FolderListResponse:
    folder = await folders.get(folder_id, owner_id)
    items = await folders.list_children(folder)
    return FolderListResponse(
        items=[FolderResponse.model_validate(item, from_attributes=True) for item in items],
        total=len(items),
    )


@router.get(
    "/{folder_id}/subtree",
    response_model=FolderListResponse,
    summary="Recursive listing via a single ltree predicate",
)
async def list_subtree(
    folder_id: uuid.UUID,
    owner_id: OwnerDep,
    folders: FolderServiceDep,
    include_self: Annotated[bool, Query()] = True,
    max_depth: Annotated[int | None, Query(ge=1, le=32)] = None,
) -> FolderListResponse:
    folder = await folders.get(folder_id, owner_id)
    items = await folders.list_subtree(folder, include_self=include_self, max_depth=max_depth)
    return FolderListResponse(
        items=[FolderResponse.model_validate(item, from_attributes=True) for item in items],
        total=len(items),
    )


@router.get(
    "/{folder_id}/breadcrumbs",
    response_model=FolderListResponse,
    summary="Ancestor chain, root first",
)
async def breadcrumbs(
    folder_id: uuid.UUID,
    owner_id: OwnerDep,
    folders: FolderServiceDep,
) -> FolderListResponse:
    folder = await folders.get(folder_id, owner_id)
    trail = await folders.ancestors(folder)
    trail.append(folder)
    return FolderListResponse(
        items=[FolderResponse.model_validate(item, from_attributes=True) for item in trail],
        total=len(trail),
    )


@router.patch(
    "/{folder_id}",
    response_model=FolderResponse,
    summary="Rename and/or move a folder, rewriting its whole sub-tree",
)
async def update_folder(
    folder_id: uuid.UUID,
    payload: FolderUpdateRequest,
    owner_id: OwnerDep,
    folders: FolderServiceDep,
    session: SessionDep,
) -> FolderResponse:
    folder = await folders.get(folder_id, owner_id)
    move_requested = "parent_id" in payload.model_fields_set
    folder = await folders.rename_or_move(
        folder,
        name=payload.name,
        new_parent_id=payload.parent_id,
        move_requested=move_requested,
    )
    await session.commit()
    return FolderResponse.model_validate(folder, from_attributes=True)


@router.delete(
    "/{folder_id}",
    response_model=FolderDeleteResponse,
    summary="Delete a folder and every descendant folder and file",
)
async def delete_folder(
    folder_id: uuid.UUID,
    owner_id: OwnerDep,
    folders: FolderServiceDep,
    files: FileServiceDep,
    session: SessionDep,
) -> FolderDeleteResponse:
    folder = await folders.get(folder_id, owner_id)
    summary = await folders.collect_subtree(folder)

    deletion = await files.delete_many(owner_id, summary.file_ids)
    await folders.delete_subtree(folder)
    await session.commit()

    return FolderDeleteResponse(
        deleted_folder_ids=summary.folder_ids,
        deleted_file_ids=deletion.file_ids,
        released_objects=deletion.released_objects,
    )
