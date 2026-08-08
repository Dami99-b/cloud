"""Hierarchical folder behaviour backed by PostgreSQL ltree."""

from __future__ import annotations

import pytest
from httpx import AsyncClient
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.folder import Folder

pytestmark = pytest.mark.integration

V1 = settings.api_v1_prefix


async def make_folder(client: AsyncClient, name: str, parent_id: str | None = None) -> dict:
    response = await client.post(f"{V1}/folders", json={"name": name, "parent_id": parent_id})
    assert response.status_code == 201, response.text
    return response.json()


async def test_ltree_extension_is_installed(session: AsyncSession) -> None:
    installed = await session.scalar(text("SELECT 1 FROM pg_extension WHERE extname = 'ltree'"))
    assert installed == 1


async def test_root_is_materialised_on_first_touch(client: AsyncClient) -> None:
    response = await client.get(f"{V1}/folders/root")
    assert response.status_code == 200

    root = response.json()
    assert root["path"] == "root"
    assert root["depth"] == 1
    assert root["is_root"] is True
    assert root["parent_id"] is None


async def test_nested_paths_are_materialised(client: AsyncClient) -> None:
    documents = await make_folder(client, "Documents")
    projects = await make_folder(client, "Projects", documents["id"])
    q3 = await make_folder(client, "Q3 Report", projects["id"])

    assert documents["path"] == "root.documents"
    assert projects["path"] == "root.documents.projects"
    assert q3["path"] == "root.documents.projects.q3_report"
    assert q3["depth"] == 4
    assert q3["name"] == "Q3 Report"


async def test_duplicate_sibling_name_conflicts(client: AsyncClient) -> None:
    await make_folder(client, "Photos")
    response = await client.post(f"{V1}/folders", json={"name": "photos"})
    assert response.status_code == 409


async def test_same_name_under_different_parents_is_allowed(client: AsyncClient) -> None:
    a = await make_folder(client, "Alpha")
    b = await make_folder(client, "Beta")
    first = await make_folder(client, "shared", a["id"])
    second = await make_folder(client, "shared", b["id"])

    assert first["path"] == "root.alpha.shared"
    assert second["path"] == "root.beta.shared"


async def test_recursive_listing_uses_one_descendant_predicate(client: AsyncClient) -> None:
    documents = await make_folder(client, "Documents")
    projects = await make_folder(client, "Projects", documents["id"])
    await make_folder(client, "Deep", projects["id"])
    await make_folder(client, "Unrelated")

    response = await client.get(f"{V1}/folders/{documents['id']}/subtree")
    assert response.status_code == 200

    paths = [item["path"] for item in response.json()["items"]]
    assert paths == [
        "root.documents",
        "root.documents.projects",
        "root.documents.projects.deep",
    ]

    excluded = await client.get(
        f"{V1}/folders/{documents['id']}/subtree", params={"include_self": "false"}
    )
    assert documents["path"] not in [i["path"] for i in excluded.json()["items"]]

    shallow = await client.get(f"{V1}/folders/{documents['id']}/subtree", params={"max_depth": 1})
    assert len(shallow.json()["items"]) == 2


async def test_breadcrumbs_walk_ancestors_root_first(client: AsyncClient) -> None:
    documents = await make_folder(client, "Documents")
    projects = await make_folder(client, "Projects", documents["id"])

    response = await client.get(f"{V1}/folders/{projects['id']}/breadcrumbs")
    assert [item["path"] for item in response.json()["items"]] == [
        "root",
        "root.documents",
        "root.documents.projects",
    ]


async def test_tree_endpoint_nests_children(client: AsyncClient) -> None:
    documents = await make_folder(client, "Documents")
    await make_folder(client, "Projects", documents["id"])

    tree = (await client.get(f"{V1}/folders/tree")).json()
    assert len(tree) == 1
    assert tree[0]["path"] == "root"
    assert tree[0]["children"][0]["path"] == "root.documents"
    assert tree[0]["children"][0]["children"][0]["path"] == "root.documents.projects"


async def test_move_rewrites_the_whole_subtree(client: AsyncClient, session: AsyncSession) -> None:
    documents = await make_folder(client, "Documents")
    projects = await make_folder(client, "Projects", documents["id"])
    deep = await make_folder(client, "Deep", projects["id"])
    archive = await make_folder(client, "Archive")

    response = await client.patch(
        f"{V1}/folders/{projects['id']}", json={"parent_id": archive["id"]}
    )
    assert response.status_code == 200
    assert response.json()["path"] == "root.archive.projects"
    assert response.json()["depth"] == 3

    moved = await client.get(f"{V1}/folders/{deep['id']}")
    assert moved.json()["path"] == "root.archive.projects.deep"
    assert moved.json()["depth"] == 4

    rows = (
        await session.execute(select(Folder.path, Folder.depth, func.nlevel(Folder.path)))
    ).all()
    for path, depth, levels in rows:
        assert depth == levels, path


async def test_rename_keeps_children_attached(client: AsyncClient) -> None:
    documents = await make_folder(client, "Documents")
    child = await make_folder(client, "Notes", documents["id"])

    renamed = await client.patch(f"{V1}/folders/{documents['id']}", json={"name": "Archive"})
    assert renamed.status_code == 200
    assert renamed.json()["path"] == "root.archive"
    assert renamed.json()["name"] == "Archive"

    moved_child = await client.get(f"{V1}/folders/{child['id']}")
    assert moved_child.json()["path"] == "root.archive.notes"


async def test_folder_cannot_move_into_its_own_descendant(client: AsyncClient) -> None:
    documents = await make_folder(client, "Documents")
    projects = await make_folder(client, "Projects", documents["id"])

    response = await client.patch(
        f"{V1}/folders/{documents['id']}", json={"parent_id": projects["id"]}
    )
    assert response.status_code == 400


async def test_root_cannot_be_renamed_or_deleted(client: AsyncClient) -> None:
    root = (await client.get(f"{V1}/folders/root")).json()

    assert (
        await client.patch(f"{V1}/folders/{root['id']}", json={"name": "nope"})
    ).status_code == 400
    assert (await client.delete(f"{V1}/folders/{root['id']}")).status_code == 400


async def test_subtree_delete_removes_every_descendant(client: AsyncClient) -> None:
    documents = await make_folder(client, "Documents")
    projects = await make_folder(client, "Projects", documents["id"])
    deep = await make_folder(client, "Deep", projects["id"])
    survivor = await make_folder(client, "Keep")

    response = await client.delete(f"{V1}/folders/{documents['id']}")
    assert response.status_code == 200

    deleted = set(response.json()["deleted_folder_ids"])
    assert deleted == {documents["id"], projects["id"], deep["id"]}

    assert (await client.get(f"{V1}/folders/{deep['id']}")).status_code == 404
    assert (await client.get(f"{V1}/folders/{survivor['id']}")).status_code == 200


async def test_path_separators_are_rejected(client: AsyncClient) -> None:
    response = await client.post(f"{V1}/folders", json={"name": "a/b"})
    assert response.status_code == 422


async def test_unknown_folder_is_404(client: AsyncClient) -> None:
    response = await client.get(f"{V1}/folders/00000000-0000-0000-0000-0000000000ff")
    assert response.status_code == 404
