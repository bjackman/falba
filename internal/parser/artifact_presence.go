package parser

import (
	"fmt"

	"github.com/bjackman/falba/internal/falba"
)

// ArtifactPresenceExtractor just returns a fixed value for any artifact. This
// is useful for stuff like dropping traces into the artifacts directory and
// then exposing a fact to report that tracing was enabled, since that might
// effect results.
type ArtifactPresenceExtractor struct {
	result falba.Value
}

func (e *ArtifactPresenceExtractor) Extract(artifact *falba.Artifact) (falba.Value, error) {
	return e.result, nil
}

func (e *ArtifactPresenceExtractor) String() string {
	return fmt.Sprintf("ArtifactPresenceExtractor{%v}", e.result)
}

type ArtifactPresenceConfig struct {
	BaseParserConfig
	Result any `json:"result":`
}
