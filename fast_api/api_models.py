"""api_models"""
from pydantic import BaseModel, Field, Required
from typing import Optional, List, Union


class QuerySource(BaseModel):
    select_query: str = Field(default=Required)
    view_name: str = Field(default=Required, max_length=128,
                           regex="^[a-z_][a-z0-9_]*$")


class Message(BaseModel):
    message: str


class Column(BaseModel):
    name: str = Field(default=Required, max_length=128,
                      regex="^[a-z_][a-z0-9_]*$")
    type: str
    format: Optional[str] = None
    partitioned: bool = False


class StaticColumn(BaseModel):
    name: str = Field(default=Required, max_length=128,
                      regex="^[a-z_][a-z0-9_]*$")
    type: str = Field(default=Required)
    format: Optional[str] = None
    value: str = Field(default=Required)


class Source(BaseModel):
    source_type: str
    source_name: str = Field(
        default=Required, max_length=128, regex="^[a-z_][a-z0-9_]*$")
    catalog_name: Optional[str] = Field(
        default="awsdatacatalog", max_length=128, regex="^[a-z_][a-z0-9_]*$")
    database_name: Optional[str] = Field(
        default=None, max_length=128, regex="^[a-z_][a-z0-9_]*$")
    table_name: str = Field(
        default=Required, max_length=128, regex="^[a-z_][a-z0-9_]*$")
    source_s3_path: Optional[str] = None
    lookup_name: Optional[str] = None
    file_name_filter: Optional[str] = None
    columns_in_sequence: List[Column] = []
    static_column: List[StaticColumn] = []

    def __str__(self):
        return self.source_name


class ColumnAsFilter(BaseModel):
    source_name: str = Field(default=Required, description="source_name")
    column_name: str = Field(default=Required, description="column_name")


class FilterValue(BaseModel):
    filter_mode: bool = Field(default=True, description="true/false")
    column_name: str = Field(
        default=Required, max_length=128, regex="^[a-z_][a-z0-9_]*$")
    condition_operator: str = Field(default="=", description="like/=/>=/<=/>/</in/is null/day-wise/min-max/window/"
                                                             "prior-business-day-period/prior-business-day/"
                                                             "previous-month-period/previous-month-last-business-day")
    filter_values: Union[str, List[str], ColumnAsFilter] = Field(default="",
                                                                 description="Filter value / list of Filter values / source Info")


class Filter(BaseModel):
    type: str = Field(default="filter", description="filter/operation/combine")
    value: Union[FilterValue, str, List[dict]] = Field(default=Required,
                                                       description="FilterValue / (or/and) / list of Filters ")


class ViewSource(BaseModel):
    source_name: str = Field(
        default=Required, max_length=128, regex="^[a-z_][a-z0-9_]*$")
    filters: List[Filter] = []


class View(BaseModel):
    source_type: str
    source_name: str = Field(
        default=Required, max_length=128, regex="^[a-z_][a-z0-9_]*$")
    table_name: str = Field(
        default=Required, max_length=128, regex="^[a-z_][a-z0-9_]*$")
    operator: str
    sources: List[ViewSource] = []


class GroupCondition(BaseModel):
    value: str
    group_value: str


class MappingValue(BaseModel):
    group_name: str = Field(
        default=Required, max_length=128, regex="^[a-z_][a-z0-9_]*$")
    default: Optional[str] = None
    column_name: str = Field(
        default=Required, max_length=128, regex="^[a-z_][a-z0-9_]*$")
    type: str = Field(default="varchar",
                      description="This is mapping value format")
    group_condition: List[GroupCondition]


class Mapping(BaseModel):
    source1: List[MappingValue] = []
    source2: List[MappingValue] = []


class Iterate(BaseModel):
    step: int = 1
    no_of_steps: Optional[int] = None


class ReconPolicy(BaseModel):
    recon_precision: int = 2
    recon_type: str = Field(default="exact-match",
                            description="exact-match/percentage-range-match(range_value)/value-range-match(range_value)"
                            )
    range_value: Optional[float] = None
    force_variance: Optional[bool] = None
    iterate: Optional[Iterate] = None


class JobDescriptionFilter(BaseModel):
    type: str = Field(default=Required, description="all/day-wise(days_ago)/min-max(min and max date)/window(days_ago)/"
                      "prior-business-day-period(days_ago)/"
                      "(months_ago)prior-business-day(days_ago)/"
                      "previous-month-period(months_ago)/"
                      "previous-month-last-business-day(months_ago)")
    days_ago: Optional[int] = None
    months_ago: Optional[int] = None
    min: Optional[str] = Field(
        default=None, description="Date format - %Y-%m-%d.")
    max: Optional[str] = Field(
        default=None, description="Date format - %Y-%m-%d.")
    source1: str = Field(default=Required, description="Source1 date column")
    source2: str = Field(default=Required, description="Source1 date column")


class JobDescription(BaseModel):
    group_name: str
    job_name: str = Field(default=Required, max_length=128,
                          regex="^[a-z_][a-z0-9_]*$")
    schedule: str = Field(
        default="@daily", description="Provide cron format schedule.")
    recon_policy: ReconPolicy
    filter: JobDescriptionFilter


class CustomColumnValue(BaseModel):
    column_name: str = Field(
        default=Required, max_length=128, regex="^[a-z_][a-z0-9_]*$")
    custom_column_query: str = Field(default=None, description="Use source name to reference column in query like - "
                                                               "lower(source_name.column_name)")
    type: str = Field(default=Required,
                      description="This is custum query column type")


class CustomColumn(BaseModel):
    source1: List[CustomColumnValue] = []
    source2: List[CustomColumnValue] = []


class JobFilter(BaseModel):
    source1: List[Filter] = []
    source2: List[Filter] = []


class SubqueryFilterValue(BaseModel):
    filter_mode: bool = Field(default=True, description="true/false")
    column_name: str = Field(
        default=Required, max_length=128, regex="^[a-z_][a-z0-9_]*$")
    condition_operator: str = Field(default="max", description="min/max")


class SubqueryFilter(BaseModel):
    type: str = Field(default="filter", description="filter/operation/combine")
    value: Union[SubqueryFilterValue, str, List[dict]] = Field(default=None,
                                                               description="SubqueryFilterValue / (or/and) / list of"
                                                                           " SubqueryFilter.")


class SubqueryFilters(BaseModel):
    source1: List[SubqueryFilter] = []
    source2: List[SubqueryFilter] = []


class ConversionFactor(BaseModel):
    column_name: str = Field(
        default=Required, max_length=128, regex="^[a-z_][a-z0-9_]*$")
    conversion_factor_column: str
    name_as: str


class LookupValue(BaseModel):
    reference_columns: List[str]
    source_name: str = Field(
        default=Required, max_length=128, regex="^[a-z_][a-z0-9_]*$")
    key_columns: List[str]
    value_columns: Optional[List[str]]
    default_column: dict = Field(default={},
                                 description="It's key-value pair where key is value column and "
                                             "value is default column(source column) for that value column.")
    conversion_factor: Optional[List[ConversionFactor]] = Field(default=None)


class Lookups(BaseModel):
    source1: List[LookupValue] = []
    source2: List[LookupValue] = []


class Coalesce(BaseModel):
    source1: dict = Field(default={}, description="It's key-value pair where key is alias name "
                                                  "and value is list of columns")
    source2: dict = Field(default={}, description="It's key-value pair where key is alias name "
                                                  "and value is list of columns")


class Key(BaseModel):
    source1: str = Field(default=Required, max_length=128,
                         regex="^[a-z_][a-z0-9_]*$")
    source2: str = Field(default=Required, max_length=128,
                         regex="^[a-z_][a-z0-9_]*$")


class Aggregation(BaseModel):
    status: bool
    operation: str = Field(default="sum", description="sum")


class ValueComparisonValue(BaseModel):
    type: str = Field(
        default=Required, description="column/operation/numeric/combine")
    value: Union[str, float, List[dict]] = Field(default=Required,
                                                 description="str(column_name, +, -, *, /) / float / list of "
                                                             "ValueComparisonValue.")


class ValueToMatchedValue(BaseModel):
    source1: List[ValueComparisonValue]
    source2: List[ValueComparisonValue]


class ValueToMatched(BaseModel):
    operation: str
    value: ValueToMatchedValue


class ExtraColumn(BaseModel):
    source1: List[str] = []
    source2: List[str] = []


class Job(BaseModel):
    source1: str = Field(default=Required, max_length=128,
                         regex="^[a-z_][a-z0-9_]*$")
    source2: str = Field(default=Required, max_length=128,
                         regex="^[a-z_][a-z0-9_]*$")
    job_description: JobDescription
    mapping: Mapping
    custom_column: CustomColumn
    filters: JobFilter
    subquery_filters: SubqueryFilters
    lookups: Lookups
    coalesce: Coalesce
    keys: List[Key]
    aggregation: Aggregation
    value_to_matched: ValueToMatched
    extra_column: ExtraColumn


class DefaultJobName(BaseModel):
    default_job_name: str


class ImportSourceList(BaseModel):
    source_list: List[str] = []


class JobData(BaseModel):
    job_data: List[dict] = []


class LatestJobTime(BaseModel):
    run_id: str
    date: str
    run_date: str
    job_run_date: str


class JobTime(BaseModel):
    run_id: str
    run_date: str
    job_run_date: str


class JobTimeList(BaseModel):
    job_time_list: List[JobTime] = []


class JobList(BaseModel):
    job_list: List[str] = []


class JobConfigList(BaseModel):
    job_config_list: List[str] = []


class SourceList(BaseModel):
    source_list: List[str] = []


class UpdateDagDetails(BaseModel):
    schedule: Optional[str] = None
    group_name: Optional[str] = None
    execution_minutes: Optional[int] = None
