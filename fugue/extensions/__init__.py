# flake8: noqa
from fugue.extensions.creator import Creator, creator, register_creator
from fugue.extensions.outputter import Outputter, outputter, register_outputter
from fugue.extensions.processor import Processor, processor, register_processor
from fugue.extensions.transformer import (
    CoTransformer,
    OutputCoTransformer,
    OutputTransformer,
    Transformer,
    cotransformer,
    output_cotransformer,
    output_transformer,
    register_output_transformer,
    register_transformer,
    transformer,
)
