"""Hard-fail error types for source registration (PRD §6, §19).

Provenance is never repaired silently: an unreadable artifact or a manifest
that violates the PRD §6 schema raises instead of being normalized or skipped,
and the caller leaves the previous manifest untouched.
"""


class SourceRegistrationError(Exception):
    """Base class for source registration failures."""


class ArtifactError(SourceRegistrationError):
    """The source artifact could not be read."""


class RawArtifactError(SourceRegistrationError):
    """Immutable Raw storage is missing, corrupted, or cannot be published."""


class ZipInventoryError(SourceRegistrationError):
    """A registered ZIP or its persisted member inventory is invalid."""


class ZipMonthMappingError(SourceRegistrationError):
    """ZIP members cannot be mapped safely to logical rental months."""


class CsvIngestionError(SourceRegistrationError):
    """A rental CSV source unit cannot be decoded or structurally interpreted."""


class SourceProjectionError(SourceRegistrationError):
    """A structurally valid source row cannot be projected under source metadata rules."""


class CanonicalizationError(SourceRegistrationError):
    """A source unit cannot be safely canonicalized under the Clean/DQ contract."""


class GroupDqError(SourceRegistrationError):
    """A rental month cannot be classified safely under the group-level DQ contract."""


class CleanPublishError(SourceRegistrationError):
    """A Clean month cannot be validated or published under the immutable publish contract."""


class StationSnapshotError(SourceRegistrationError):
    """A station snapshot cannot be safely interpreted, consolidated, or published."""


class StationHistoryError(SourceRegistrationError):
    """The observed station-history warehouse dimension cannot be built or published safely."""


class TripStationEnrichmentError(SourceRegistrationError):
    """Trusted Clean trips cannot be evaluated safely against observed station history."""


class FactTripPublishError(SourceRegistrationError):
    """A trusted trip fact month cannot be validated or published safely."""


class DateDimensionError(SourceRegistrationError):
    """The deterministic production date dimension cannot be built or published safely."""


class StationDayMartError(SourceRegistrationError):
    """The station-day mart cannot be built or published safely."""


class OdDayMartError(SourceRegistrationError):
    """The origin-destination day mart cannot be built or published safely."""


class SourceMonthQualityMartError(SourceRegistrationError):
    """The source-month quality mart cannot be built or published safely."""


class BackfillError(SourceRegistrationError):
    """A month/range backfill plan or execution cannot satisfy the PRD contract."""


class ManifestError(SourceRegistrationError):
    """The manifest is unreadable, malformed, or off the PRD §6 schema."""
