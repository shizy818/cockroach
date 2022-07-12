package tree

// CreateModel represents a CREATE MODEL statement.
type CreateModel struct {
	Name Name
	From From
}

// Format implements the NodeFormatter interface.
func (node *CreateModel) Format(ctx *FmtCtx) {
	ctx.WriteString("CREATE MODEL\n")
	if node.Name != "" {
		ctx.FormatNode(&node.Name)
		ctx.WriteByte(' ')
	}

	ctx.FormatNode(&node.From)
}
