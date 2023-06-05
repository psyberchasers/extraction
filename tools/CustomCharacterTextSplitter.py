from typing import Any, List, Optional
from langchain.text_splitter import RecursiveCharacterTextSplitter


class CustomCharacterTextSplitter(RecursiveCharacterTextSplitter):
    """Implementation of splitting text that looks at characters.

    Recursively tries to split by different characters to find one
    that works.
    """

    def __init__(
        self,
        max_chunks: int = 50,
        separators: Optional[List[str]] = None,
        **kwargs: Any
    ):
        """Create a new TextSplitter."""
        super().__init__(**kwargs)
        self._max_chunks = max_chunks
        self._separators = separators or ["\n\n", "\n", " ", ""]

    def split_text(self, text: str) -> List[str]:
        """Split incoming text and return chunks."""
        final_chunks = []
        # Get appropriate separator to use
        separator = self._separators[-1]
        for _s in self._separators:
            if _s == "":
                separator = _s
                break
            if _s in text:
                separator = _s
                break
        # Now that we have the separator, split the text
        if separator:
            splits = text.split(separator)
        else:
            splits = list(text)
        # Now go merging things, recursively splitting longer texts.
        _good_splits = []
        for s in splits:
            if self._length_function(s) < self._chunk_size:
                _good_splits.append(s)
            else:
                if _good_splits:
                    merged_text = self._merge_splits(_good_splits, separator)
                    final_chunks.extend(merged_text)
                    _good_splits = []
                if len(final_chunks) >= self._max_chunks:
                    break
                other_info = self.split_text(s)
                final_chunks.extend(other_info)
            if len(final_chunks) >= self._max_chunks:
                break
        if _good_splits:
            merged_text = self._merge_splits(_good_splits, separator)
            final_chunks.extend(merged_text)
        return final_chunks[: self._max_chunks]
