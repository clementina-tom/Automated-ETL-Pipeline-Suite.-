class ETLPipelineError(Exception):
    """Base ETL pipeline exception."""


class ExtractionError(ETLPipelineError):
    pass


class ValidationError(ETLPipelineError):
    pass


class LoadingError(ETLPipelineError):
    pass


class SchemaEvolutionError(ETLPipelineError):
    pass
