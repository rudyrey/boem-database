/**
 * Pagination middleware — parses ?page, ?limit, ?sort, ?order from query string.
 * Attaches parsed values to req.pagination.
 */
function pagination(allowedSorts = []) {
  return (req, res, next) => {
    const page = Math.max(1, parseInt(req.query.page) || 1);
    const limit = Math.min(500, Math.max(1, parseInt(req.query.limit) || 50));
    const offset = (page - 1) * limit;

    let sort = req.query.sort || null;
    const order = (req.query.order || 'asc').toUpperCase() === 'DESC' ? 'DESC' : 'ASC';

    // Validate sort column against whitelist
    if (sort && allowedSorts.length > 0 && !allowedSorts.includes(sort)) {
      sort = null;
    }

    req.pagination = { page, limit, offset, sort, order };
    next();
  };
}

/**
 * Build paginated response envelope.
 */
function paginatedResponse(data, total, { page, limit }) {
  return {
    data,
    pagination: {
      page,
      limit,
      total,
      totalPages: Math.ceil(total / limit),
    },
  };
}

module.exports = { pagination, paginatedResponse };
