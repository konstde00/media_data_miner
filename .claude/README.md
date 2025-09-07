# Claude Code Configuration for reddit-data-miner

## Serena Integration Status

This project is configured with Serena for enhanced semantic code analysis. However, there may be connection issues with the MCP server.

### Manual Serena Usage

If the MCP integration fails, you can still use Serena manually:

```bash
# Health check
export PATH="$HOME/.local/bin:$PATH" && uvx --from git+https://github.com/oraios/serena serena project health-check

# Re-index after changes
export PATH="$HOME/.local/bin:$PATH" && uvx --from git+https://github.com/oraios/serena serena project index

# Manual server start for testing
export PATH="$HOME/.local/bin:$PATH" && uvx --from git+https://github.com/oraios/serena serena start-mcp-server --project $(pwd) --context desktop-app
```

### Project Structure

Key files and directories:
- `app/` - Main application modules (config, retrieval, utils, main)
- `scripts/` - Data processing scripts (ingest, parallel processing, job queue)
- `quick_scrape.py` - Quick data scraping utility
- `continuous_scrape.py` - Continuous scraping process
- `.serena/` - Serena configuration and cache

### Recommended Tool Priorities

When Serena MCP is working, prioritize these tools for:
- **Code Navigation**: Use Serena's `find_symbol` and `get_symbols_overview`
- **Cross-References**: Use `find_referencing_symbols` for understanding dependencies
- **Search**: Use `search_for_pattern` for semantic search across the codebase
- **File Operations**: Use standard Claude Code tools (Read, Write, Edit)

### Environment Variables

Set `CLAUDE_SERENA_PRIORITY=true` to signal Serena availability to future Claude Code instances.