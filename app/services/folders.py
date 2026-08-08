"""Hierarchical folder operations backed by PostgreSQL `ltree`.

Every folder stores its full materialised path (``root.documents.projects``).
That makes the three expensive tree operations index-only scans against the
GiST index rather than recursive CTEs:

* **recursive listing** - ``path <@ 'root.documents'``
* **sub-tree delete**   - the same predicate, one statement
* **move / rename**     - one UPDATE rewriting the prefix of the whole sub-tree

Labels are slugified from the human name and de-duplicated among siblings, so
paths stay readable while remaining valid ltree labels.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass

from sqlalchemy import String, cast, delete, func, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.errors import BadRequestError, ConflictError, NotFoundError
from app.core.logging import get_logger
from app.db.types import MAX_DEPTH, join_path, slugify_label
from app.models.file import File
from app.models.folder import ROOT_LABEL, Folder

logger = get_logger(__name__)


@dataclass(slots=True)
class SubtreeSummary:
    folder_ids: list[uuid.UUID]
    file_ids: list[uuid.UUID]
    storage_keys: list[str]


class FolderService:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def get(self, folder_id: uuid.UUID, owner_id: uuid.UUID) -> Folder:
        folder = await self.session.scalar(
            select(Folder).where(Folder.id == folder_id, Folder.owner_id == owner_id)
        )
        if folder is None:
            raise NotFoundError("folder not found", details={"folder_id": str(folder_id)})
        return folder

    async def get_root(self, owner_id: uuid.UUID) -> Folder:
        """Fetch - or lazily create - the owner's root folder."""
        root = await self.session.scalar(
            select(Folder).where(Folder.owner_id == owner_id, Folder.is_root.is_(True))
        )
        if root is not None:
            return root

        root = Folder(
            owner_id=owner_id,
            parent_id=None,
            name="root",
            label=ROOT_LABEL,
            path=ROOT_LABEL,
            depth=1,
            is_root=True,
        )
        self.session.add(root)
        try:
            await self.session.flush()
        except IntegrityError:
            await self.session.rollback()
            existing = await self.session.scalar(
                select(Folder).where(Folder.owner_id == owner_id, Folder.is_root.is_(True))
            )
            if existing is None:
                raise
            return existing
        return root

    async def resolve(self, owner_id: uuid.UUID, folder_id: uuid.UUID | None) -> Folder:
        """`None` means 'the root folder' everywhere in the API."""
        if folder_id is None:
            return await self.get_root(owner_id)
        return await self.get(folder_id, owner_id)

    async def _unique_label(self, parent_id: uuid.UUID, name: str) -> str:
        base = slugify_label(name)
        taken = set(
            (
                await self.session.scalars(
                    select(Folder.label).where(Folder.parent_id == parent_id)
                )
            ).all()
        )
        if base not in taken:
            return base
        for suffix in range(2, 1000):
            candidate = f"{base}_{suffix}"
            if candidate not in taken:
                return candidate
        raise ConflictError("too many sibling folders with a similar name")

    async def create(
        self,
        owner_id: uuid.UUID,
        *,
        name: str,
        parent_id: uuid.UUID | None = None,
    ) -> Folder:
        parent = await self.resolve(owner_id, parent_id)

        if parent.depth >= MAX_DEPTH:
            raise BadRequestError(
                f"folder nesting is limited to {MAX_DEPTH} levels",
                details={"parent_path": parent.path},
            )

        duplicate = await self.session.scalar(
            select(Folder.id).where(
                Folder.parent_id == parent.id,
                func.lower(Folder.name) == name.lower(),
            )
        )
        if duplicate is not None:
            raise ConflictError(
                "a folder with that name already exists here",
                details={"parent_path": parent.path, "name": name},
            )

        label = await self._unique_label(parent.id, name)
        folder = Folder(
            owner_id=owner_id,
            parent_id=parent.id,
            name=name,
            label=label,
            path=join_path(parent.path, label),
            depth=parent.depth + 1,
            is_root=False,
        )
        self.session.add(folder)
        await self.session.flush()
        logger.info("folder created", extra={"folder_id": str(folder.id), "path": folder.path})
        return folder

    async def list_children(self, folder: Folder) -> list[Folder]:
        result = await self.session.scalars(
            select(Folder).where(Folder.parent_id == folder.id).order_by(Folder.name)
        )
        return list(result.all())

    async def list_subtree(
        self,
        folder: Folder,
        *,
        include_self: bool = True,
        max_depth: int | None = None,
    ) -> list[Folder]:
        """Recursive listing - one indexed ``<@`` predicate, any depth."""
        stmt = select(Folder).where(
            Folder.owner_id == folder.owner_id,
            Folder.path.descendant_of(folder.path),
        )
        if not include_self:
            stmt = stmt.where(Folder.id != folder.id)
        if max_depth is not None:
            stmt = stmt.where(func.nlevel(Folder.path) <= folder.depth + max_depth)
        stmt = stmt.order_by(cast(Folder.path, String))
        return list((await self.session.scalars(stmt)).all())

    async def list_all(self, owner_id: uuid.UUID) -> list[Folder]:
        result = await self.session.scalars(
            select(Folder).where(Folder.owner_id == owner_id).order_by(cast(Folder.path, String))
        )
        return list(result.all())

    async def file_counts(self, owner_id: uuid.UUID) -> dict[uuid.UUID, int]:
        from app.models.enums import FileStatus

        rows = await self.session.execute(
            select(File.folder_id, func.count(File.id))
            .where(File.owner_id == owner_id, File.status != FileStatus.DELETED)
            .group_by(File.folder_id)
        )
        return dict(rows.all())

    async def ancestors(self, folder: Folder) -> list[Folder]:
        """Breadcrumb trail, root first, excluding the folder itself."""
        result = await self.session.scalars(
            select(Folder)
            .where(
                Folder.owner_id == folder.owner_id,
                Folder.path.ancestor_of(folder.path),
                Folder.id != folder.id,
            )
            .order_by(func.nlevel(Folder.path))
        )
        return list(result.all())

    async def rename_or_move(
        self,
        folder: Folder,
        *,
        name: str | None = None,
        new_parent_id: uuid.UUID | None = None,
        move_requested: bool = False,
    ) -> Folder:
        if folder.is_root:
            raise BadRequestError("the root folder cannot be renamed or moved")

        old_path = folder.path
        parent = (
            await self.resolve(folder.owner_id, new_parent_id)
            if move_requested
            else await self.get(folder.parent_id, folder.owner_id)  # type: ignore[arg-type]
        )

        if move_requested:
            if parent.id == folder.id:
                raise BadRequestError("a folder cannot be moved into itself")
            if parent.path == old_path or parent.path.startswith(f"{old_path}."):
                raise BadRequestError("a folder cannot be moved into its own descendant")

        new_name = name or folder.name
        sibling_changed = move_requested or (name is not None and name != folder.name)
        if sibling_changed:
            clash = await self.session.scalar(
                select(Folder.id).where(
                    Folder.parent_id == parent.id,
                    func.lower(Folder.name) == new_name.lower(),
                    Folder.id != folder.id,
                )
            )
            if clash is not None:
                raise ConflictError(
                    "a folder with that name already exists in the destination",
                    details={"parent_path": parent.path, "name": new_name},
                )

        new_label = folder.label
        if name is not None and name != folder.name:
            new_label = await self._unique_label(parent.id, name)
        elif move_requested:
            new_label = await self._unique_label(parent.id, folder.name)

        new_path = join_path(parent.path, new_label)
        if new_path == old_path and new_name == folder.name:
            return folder

        subtree_depth = await self.session.scalar(
            select(func.max(func.nlevel(Folder.path))).where(
                Folder.owner_id == folder.owner_id,
                Folder.path.descendant_of(old_path),
            )
        )
        relative_depth = int(subtree_depth or folder.depth) - folder.depth
        if len(new_path.split(".")) + relative_depth > MAX_DEPTH:
            raise BadRequestError(f"the move would exceed the {MAX_DEPTH}-level nesting limit")

        old_levels = folder.depth
        new_levels = len(new_path.split("."))
        await self.session.execute(
            update(Folder)
            .where(
                Folder.owner_id == folder.owner_id,
                Folder.path.descendant_of(old_path),
                Folder.id != folder.id,
            )
            .values(
                path=func.text2ltree(new_path).concat(func.subpath(Folder.path, old_levels)),
                depth=func.nlevel(Folder.path) - old_levels + new_levels,
            )
            .execution_options(synchronize_session=False)
        )
        await self.session.execute(
            update(Folder)
            .where(Folder.id == folder.id)
            .values(path=func.text2ltree(new_path), depth=new_levels)
            .execution_options(synchronize_session=False)
        )

        folder.name = new_name
        folder.label = new_label
        folder.parent_id = parent.id
        await self.session.flush()
        await self.session.refresh(folder)
        logger.info(
            "folder moved",
            extra={"folder_id": str(folder.id), "from": old_path, "to": folder.path},
        )
        return folder

    async def collect_subtree(self, folder: Folder) -> SubtreeSummary:
        """Everything that a sub-tree delete would touch."""
        folder_ids = list(
            (
                await self.session.scalars(
                    select(Folder.id).where(
                        Folder.owner_id == folder.owner_id,
                        Folder.path.descendant_of(folder.path),
                    )
                )
            ).all()
        )
        rows = (
            await self.session.execute(
                select(File.id, File.storage_key).where(File.folder_id.in_(folder_ids))
            )
        ).all()
        return SubtreeSummary(
            folder_ids=folder_ids,
            file_ids=[row[0] for row in rows],
            storage_keys=[row[1] for row in rows],
        )

    async def delete_subtree(self, folder: Folder) -> SubtreeSummary:
        """Delete a folder and everything beneath it.

        Files are removed by the caller first (so blob refcounts are released
        and orphaned S3 objects reclaimed); this drops the folder rows.
        """
        if folder.is_root:
            raise BadRequestError("the root folder cannot be deleted")

        summary = await self.collect_subtree(folder)
        await self.session.execute(
            delete(Folder).where(
                Folder.owner_id == folder.owner_id,
                Folder.path.descendant_of(folder.path),
            )
        )
        await self.session.flush()
        logger.info(
            "folder sub-tree deleted",
            extra={
                "path": folder.path,
                "folders": len(summary.folder_ids),
                "files": len(summary.file_ids),
            },
        )
        return summary
