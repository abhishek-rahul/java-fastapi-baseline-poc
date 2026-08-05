from pydantic import BaseModel, Field


class ProcessRequest(BaseModel):
    request_id: str = Field(alias="requestId", min_length=1)
    message: str = Field(min_length=1)
    delay_ms: int = Field(default=100, alias="delayMs", ge=0, le=10_000)

    model_config = {"populate_by_name": True}


class ProcessResponse(BaseModel):
    request_id: str = Field(alias="requestId")
    original_message: str = Field(alias="originalMessage")
    processed_message: str = Field(alias="processedMessage")
    delay_ms: int = Field(alias="delayMs")
    python_start_time: str = Field(alias="pythonStartTime")
    python_end_time: str = Field(alias="pythonEndTime")
    python_execution_time_ms: float = Field(alias="pythonExecutionTimeMs")
    event_loop_thread: str = Field(alias="eventLoopThread")

    model_config = {"populate_by_name": True}
