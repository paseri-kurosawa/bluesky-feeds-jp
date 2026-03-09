"""
Density-based content quality scoring module using .ftz model.

Simple, lightweight scoring:
1. zlib compression ratio check (noise detection)
2. Word vector norm extraction from .ftz model
"""

import zlib
import os
import re
import math
from typing import List, Tuple

# Global scope for warm starts
_ft_model = None
_janome_tokenizer = None

def get_fasttext_model():
    """Lazy load fastText .ftz model (pre-cached in Lambda image)"""
    global _ft_model
    if _ft_model is None:
        import fasttext
        import urllib.request

        # Use language detection model for word vectors (lightweight)
        model_path = "/tmp/lid.176.ftz"
        if not os.path.exists(model_path):
            print("[FASTTEXT_LOAD] Downloading lid.176.ftz...")
            urllib.request.urlretrieve(
                "https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.ftz",
                model_path
            )

        print("[FASTTEXT_LOAD] Loading .ftz model...")
        _ft_model = fasttext.load_model(model_path)
        print("[FASTTEXT_LOAD] Model loaded successfully")

    return _ft_model

def get_janome_tokenizer():
    """Lazy load Janome tokenizer (cached for warm starts)"""
    global _janome_tokenizer
    if _janome_tokenizer is None:
        from janome.tokenizer import Tokenizer
        print("[JANOME_LOAD] Initializing Janome tokenizer...")
        _janome_tokenizer = Tokenizer()
        print("[JANOME_LOAD] Tokenizer loaded successfully")
    return _janome_tokenizer

def calculate_compression_ratio(text: str) -> float:
    """
    Calculate zlib compression ratio.

    Args:
        text: Input text

    Returns:
        Compression ratio (compressed_size / original_size)
    """
    if not text:
        return 1.0

    original_bytes = text.encode('utf-8')
    compressed_bytes = zlib.compress(original_bytes, level=9)

    ratio = len(compressed_bytes) / len(original_bytes)
    return ratio

def tokenize_japanese(text: str) -> List[str]:
    """
    Japanese tokenization using Janome morphological analyzer.

    Extracts content words only:
    - 名詞 (nouns)
    - 動詞 (verbs)
    - 形容詞 (adjectives)
    - 副詞 (adverbs)

    Filters out:
    - Particles (助詞)
    - Auxiliary verbs (助動詞)
    - Symbols (記号)
    - Non-content nouns: pronouns, numbers, suffixes, non-independent nouns

    Args:
        text: Input text

    Returns:
        List of content word surface forms
    """
    tokenizer = get_janome_tokenizer()
    content_words = []

    # Target POS tags (part-of-speech)
    target_pos_prefixes = ('名詞', '動詞', '形容詞', '副詞')

    # Exclude specific noun subtypes that aren't content-bearing
    exclude_pos_patterns = (
        '名詞,非自立',      # Non-independent nouns
        '名詞,代名詞',      # Pronouns
        '名詞,数',          # Numbers
        '名詞,接尾',        # Suffixes
    )

    try:
        for token in tokenizer.tokenize(text):
            # token.part_of_speech: "品詞,品詞細分類1,品詞細分類2,..."
            # token.surface: surface form (表層形)
            pos = token.part_of_speech
            surface = token.surface

            # Skip empty or whitespace-only tokens
            if not surface or not surface.strip():
                continue

            # Check if POS starts with target categories
            if not any(pos.startswith(prefix) for prefix in target_pos_prefixes):
                continue

            # Exclude non-content noun subtypes
            if any(pos.startswith(pattern) for pattern in exclude_pos_patterns):
                continue

            # Length filter: exclude very short tokens (1 character)
            # Exception: kanji single characters are often meaningful
            if len(surface) == 1 and not ('\u4e00' <= surface <= '\u9fff'):
                continue

            content_words.append(surface)

    except Exception as e:
        print(f"[TOKENIZE_ERROR] Janome error: {e}")
        import traceback
        traceback.print_exc()
        # Fallback: return empty list (will result in score=0)
        return []

    print(f"[JANOME_TOKENS] Extracted {len(content_words)} content words")
    return content_words

def extract_word_vector_norms(text: str) -> List[Tuple[str, float]]:
    """
    Extract word vector norms using .ftz model.

    Args:
        text: Input text

    Returns:
        List of (word, vector_norm) tuples
    """
    ft_model = get_fasttext_model()
    tokens = tokenize_japanese(text)
    words_with_norms = []

    try:
        for token_idx, token in enumerate(tokens):
            # Debug: log first 5 tokens
            if token_idx < 5:
                try:
                    vector = ft_model.get_word_vector(token)
                    norm = float((vector ** 2).sum() ** 0.5)
                    print(f"[TOKEN_DEBUG] idx={token_idx}, text='{token}', vector_norm={norm:.4f}")
                except Exception as e:
                    print(f"[TOKEN_DEBUG] idx={token_idx}, text='{token}', error={e}")

            # Get word vector and compute norm
            try:
                vector = ft_model.get_word_vector(token)
                norm = float((vector ** 2).sum() ** 0.5)
                if norm > 0:
                    words_with_norms.append((token, norm))
            except Exception as e:
                print(f"[VECTOR_ERROR] token='{token}': {e}")

    except Exception as e:
        print(f"[ERROR] Error in extract_word_vector_norms: {e}")
        import traceback
        traceback.print_exc()
        return []

    print(f"[VECTORS_EXTRACTED] {len(words_with_norms)} tokens with vectors out of {len(tokens)}")
    return words_with_norms

def calculate_density_score(text: str) -> float:
    """
    Calculate density score for a text.

    Algorithm:
    - Step 1: Check zlib compression ratio (noise detection)
    - Step 2: Extract word vectors using .ftz model
    - Step 3: Calculate average vector norm

    Args:
        text: Input text

    Returns:
        Density score (0-1 scale), or 0 if text fails checks
    """
    try:
        # Step 1: Compression ratio check
        comp_ratio = calculate_compression_ratio(text)

        # Noise detection: ratio too low (random) or too high (repetitive)
        if comp_ratio < 0.20 or comp_ratio > 0.95:
            print(f"[DENSITY] Failed compression check (ratio={comp_ratio:.3f})")
            return 0.0

        # Step 2: Extract word vectors
        words_with_norms = extract_word_vector_norms(text)

        if not words_with_norms:
            print("[DENSITY] No word vectors found")
            return 0.0

        # Step 3: Calculate average norm and normalize
        norms = [norm for _, norm in words_with_norms]
        avg_norm = sum(norms) / len(norms)
        min_norm = min(norms)
        max_norm = max(norms)

        # Normalize to 0-1 scale using sigmoid
        # Sigmoid function: 1 / (1 + e^(-k*(x - x0)))
        # where x0 = midpoint (avg_norm where output = 0.5), k = steepness
        # x0=8 (center around observed average), k=0.5 (moderate steepness)
        sigmoid_midpoint = 8.0
        sigmoid_steepness = 0.5
        normalized_score = 1.0 / (1.0 + math.exp(-sigmoid_steepness * (avg_norm - sigmoid_midpoint)))

        # Detailed analysis log
        print(f"[VECTOR_ANALYSIS] min={min_norm:.4f}, max={max_norm:.4f}, avg={avg_norm:.4f}, token_count={len(words_with_norms)}")
        print(f"[DENSITY_SCORE] {normalized_score:.3f} (tokens={len(words_with_norms)}, avg_norm={avg_norm:.4f}, comp_ratio={comp_ratio:.3f})")
        return normalized_score

    except Exception as e:
        print(f"[DENSITY_ERROR] Error: {e}")
        import traceback
        traceback.print_exc()
        return 0.0
