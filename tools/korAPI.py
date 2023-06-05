"""Kor API for extraction related functionality."""
import asyncio
from typing import Any, Callable, List, Optional, Sequence, Type, Union, cast

from langchain import PromptTemplate
from langchain.chains import LLMChain
from langchain.docstore.document import Document


from kor.extraction.typedefs import DocumentExtraction, Extraction


async def _extract_from_document_with_semaphore(
    semaphore: asyncio.Semaphore,
    chain: LLMChain,
    document: Document,
    uid: str,
    source_uid: str,
) -> DocumentExtraction:
    """Extract from document with a semaphore to limit concurrency."""
    async with semaphore:
        extraction_result: Extraction = cast(
            Extraction, await chain.apredict_and_parse(text=document.page_content)
        )
        return extraction_result["data"]


async def extract_from_documents(
    chain: LLMChain,
    documents: Sequence[Document],
    *,
    max_concurrency: int = 1,
    use_uid: bool = False,
    extraction_uid_function: Optional[Callable[[Document], str]] = None,
    return_exceptions: bool = False,
) -> List[Union[DocumentExtraction, Exception]]:
    """Run extraction through all the given documents.

    Attention: When using this function with a large number of documents, mind the bill
               since this can use a lot of tokens!

    Concurrency is currently limited using a semaphore. This is a temporary
    and can be changed to a queue implementation to support a non-materialized stream
    of documents.

    Args:
        chain: the extraction chain to use for extraction
        documents: the documents to run extraction on
        max_concurrency: the maximum number of concurrent requests to make,
                         uses a semaphore to limit concurrency
        use_uid: If True, will use a uid attribute in metadata if it exists
                          will raise error if attribute does not exist.
                 If False, will use the index of the document in the list as the uid
        extraction_uid_function: Optional function to use to generate the uid for
             a given DocumentExtraction. If not provided, will use the uid
             of the document.
        return_exceptions: named argument passed to asyncio.gather

    Returns:
        A list of extraction results
        if return_exceptions = True, the exceptions may be returned as well.
    """
    semaphore = asyncio.Semaphore(value=max_concurrency)

    tasks = []
    for idx, doc in enumerate(documents):
        if use_uid:
            source_uid = doc.metadata.get("uid")
            if source_uid is None:
                raise ValueError(
                    f"uid not found in document metadata for document {idx}"
                )
            # C
            source_uid = str(source_uid)
        else:
            source_uid = str(idx)

        extraction_uid = (
            extraction_uid_function(doc) if extraction_uid_function else source_uid
        )

        tasks.append(
            asyncio.ensure_future(
                _extract_from_document_with_semaphore(
                    semaphore, chain, doc, extraction_uid, source_uid
                )
            )
        )

    results = await asyncio.gather(*tasks, return_exceptions=return_exceptions)
    return results
