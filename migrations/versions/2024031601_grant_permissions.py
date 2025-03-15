from alembic import op

# Revision identifiers
revision = '2024031601'
down_revision = None  # Change this if it depends on a previous migration
branch_labels = None
depends_on = None

def upgrade():
    """Apply schema permissions"""
    op.execute("GRANT CONNECT ON DATABASE my_db TO etl_app;")
    op.execute("GRANT USAGE, CREATE ON SCHEMA public TO etl_app;")
    op.execute("ALTER ROLE etl_app SET search_path TO public;")

def downgrade():
    """Rollback schema permissions (optional)"""
    op.execute("REVOKE USAGE, CREATE ON SCHEMA public FROM etl_app;")
    op.execute("ALTER ROLE etl_app SET search_path TO '';")
