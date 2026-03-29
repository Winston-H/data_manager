from pydantic import BaseModel, ConfigDict, Field, model_validator


class QueryRequest(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "description": (
                "至少提供 name_keyword 或 id_no_keyword 之一。"
                "year_prefix/year_start/year_end 为可选附加筛选条件。"
                "姓名单字按首字匹配，多字按完整姓名匹配；身份证号完整18位按整证精确匹配，否则按前4位匹配。"
            )
        }
    )

    name_keyword: str | None = Field(default=None, description="姓名查询词。单字时按首字匹配，多字时按完整姓名匹配。")
    id_no_keyword: str | None = Field(
        default=None,
        description="身份证号查询词。完整18位按整证精确匹配，否则仅使用前4位进行前缀匹配。",
    )
    year_prefix: str | None = Field(default=None, description="年份前缀筛选，如 196 匹配 1960-1969")
    year_start: int | None = Field(default=None, description="年份范围起点，需与姓名或身份证查询组合使用")
    year_end: int | None = Field(default=None, description="年份范围终点，需与姓名或身份证查询组合使用")

    @model_validator(mode="after")
    def validate_query(self):
        self.name_keyword = self.name_keyword.strip() if isinstance(self.name_keyword, str) else self.name_keyword
        self.id_no_keyword = self.id_no_keyword.strip() if isinstance(self.id_no_keyword, str) else self.id_no_keyword
        self.year_prefix = self.year_prefix.strip() if isinstance(self.year_prefix, str) else self.year_prefix
        if self.name_keyword == "":
            self.name_keyword = None
        if self.id_no_keyword == "":
            self.id_no_keyword = None
        if self.year_prefix == "":
            self.year_prefix = None
        if self.year_prefix is not None:
            if not self.year_prefix.isdigit():
                raise ValueError("year_prefix must contain digits only")
            if len(self.year_prefix) > 4:
                raise ValueError("year_prefix length must be <= 4")

        if not self.name_keyword and not self.id_no_keyword:
            raise ValueError("name_keyword or id_no_keyword is required")
        if self.year_start is not None and self.year_end is not None and self.year_start > self.year_end:
            raise ValueError("year_start must be <= year_end")
        return self


class QueryResponse(BaseModel):
    data: list[dict]
    meta: dict
