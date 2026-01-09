from typing import List

# Further optimized for Azure OpenAI responses
MAX_TOKENS_PER_CHUNK = 600  # Reduced from 1000 - leaves more room for response
APPROX_CHARS_PER_TOKEN = 4  # heuristic
MAX_CHARS_PER_CHUNK = MAX_TOKENS_PER_CHUNK * APPROX_CHARS_PER_TOKEN  # 2400 chars


def chunk_text(text: str) -> List[str]:
    """
    Split large text into smaller chunks suitable for LLM processing.
    Strategy:
    1. If text is small → return as single chunk
    2. Otherwise → split by paragraph boundaries
    3. Fall back to character slicing if needed
    
    Returns:
        List[str]: list of text chunks
    """
    if not text:
        return []
    
    # Fast path for small input
    if len(text) <= MAX_CHARS_PER_CHUNK:
        return [text]
    
    chunks: List[str] = []
    current_chunk: List[str] = []
    current_length = 0
    
    # Split by paragraphs first (safer semantic boundaries)
    paragraphs = text.split("\n\n")
    
    for para in paragraphs:
        para = para.strip()
        if not para:
            continue
        
        para_length = len(para)
        
        # If paragraph itself is too large, split by characters
        if para_length > MAX_CHARS_PER_CHUNK:
            _flush_chunk(chunks, current_chunk)
            current_length = 0
            
            for i in range(0, para_length, MAX_CHARS_PER_CHUNK):
                chunks.append(para[i : i + MAX_CHARS_PER_CHUNK])
            continue
        
        # If adding paragraph exceeds chunk size, flush current chunk
        if current_length + para_length > MAX_CHARS_PER_CHUNK:
            _flush_chunk(chunks, current_chunk)
            current_length = 0
        
        current_chunk.append(para)
        current_length += para_length
    
    # Flush remaining content
    _flush_chunk(chunks, current_chunk)
    
    return chunks


def _flush_chunk(chunks: List[str], current_chunk: List[str]) -> None:
    """
    Helper to flush accumulated paragraphs into chunks list.
    """
    if current_chunk:
        chunks.append("\n\n".join(current_chunk))
        current_chunk.clear()